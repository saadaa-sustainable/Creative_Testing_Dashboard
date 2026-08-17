# Partnership Auto-Invite — Flow Spec

**One-liner.** When a creator's `post_link` lands in `saadaa-creatorhub`, the
system auto-sends them a Meta Partnership Ad invite (subject to safety checks)
and tracks the response.

---

## 1. End-to-end overview

```
   ╔═════════════════════════════════════════════════════════════╗
   ║              SAADAA  PARTNERSHIP  AUTO-INVITE               ║
   ╚═════════════════════════════════════════════════════════════╝

    ┌──────────────────┐
    │  Creator content │
    │  lives on  IG    │
    └────────┬─────────┘
             │ post URL captured by Saadaa team
             ▼
    ┌──────────────────────┐
    │ saadaa-creatorhub    │   ← Supabase table (cleaned_data /
    │  row.post_link IS    │     creators / ig_data_historic).
    │  NOT NULL            │     New rows are the trigger.
    └────────┬─────────────┘
             │ polled every N minutes (or trigger-driven)
             ▼
    ┌──────────────────────┐
    │ auto_invite_partner- │
    │   ship.py (runner)   │   ← what we're documenting
    └────────┬─────────────┘
             │ per row, runs the four-gate flow ↓
             ▼
       ┌───────────────────────────────────────────┐
       │  G1  Resolve @handle  from  post_link     │
       │  G2  Verify @handle exists on Instagram   │
       │  G3  Idempotency check (already invited?) │
       │  G4  Send invite via Meta Partnership API │
       └────────┬──────────────────────────────────┘
                │
                ▼
    ┌──────────────────────┐
    │ Local SQLite +       │   ← write outcome (status, permission_id,
    │ optional creator-    │     timestamps). Run audit log to disk.
    │ hub write-back       │
    └──────────────────────┘
```

---

## 2. Trigger

A creator becomes eligible for an auto-invite when **one of these is true**:

```
   ▸  A new row is inserted into the source table with post_link populated
   ▸  An existing row is updated and post_link transitions from NULL → URL
```

The runner polls every 15 minutes by default. It skips any creator already
present in `partnership_status_local.sqlite.reachout` so duplicates never go
out.

---

## 3. Four gates — what runs per creator

### Gate 1 · Resolve `@handle` from `post_link`

```
   input :   https://www.instagram.com/<HANDLE>/p/<POST_ID>/
   regex :   ^https?://(?:www\.)?instagram\.com/([^/?#]+)/
   output:   @<HANDLE>   (lowercased, no @ prefix)

   Fallback: if the row already carries a `username` column, use that
             and ignore the URL.
```

### Gate 2 · Verify the handle exists on Instagram

```
   call:    GET /17841412619002528
            ?fields=business_discovery.username(<HANDLE>){username,id}

   pass :   IG returns a user object       → continue
   fail :   IG returns "user not found"    → record reachout(status='not_on_ig')
                                              and stop for this creator
```

This protects the daily API budget from invalid handles and gives clean
"not on IG" telemetry.

### Gate 3 · Idempotency — has this creator already been invited?

```
   local : SQLite lookup on reachout.handle
   remote: GET /17841412619002528/branded_content_ad_permissions
           ?creator_username=<HANDLE>

   PENDING_APPROVAL / APPROVED  → skip (don't re-invite)
   REJECTED / REVOKED + ≥30 d old → allowed to re-invite
   No prior record               → continue
```

### Gate 4 · Send the invite

```
   POST  /17841412619002528/branded_content_ad_permissions
   body
       creator_instagram_username = <HANDLE>
       access_token               = META_ACCESS_TOKEN

   200 OK   → response {"id": "<permission_id>"}
              record reachout(status='PENDING_APPROVAL', permission_id)
   4xx err  → record reachout(status='<error_code>', note=<error message>)
```

---

## 4. Decision tree per creator (compact form)

```
   ┌──────────────────────┐
   │  read row from CH    │
   └─────────┬────────────┘
             │
             ▼
       extract handle ───── no/invalid URL ──► SKIP  (status: 'bad_url')
             │
             ▼
       BizDiscovery probe ── 404 ─────────► RECORD (status: 'not_on_ig')
             │
             ▼
       existing invite? ── pending / approved ─► SKIP (status: existing)
             │
             ▼
       within cool-off?  ── yes ─────────► SKIP (status: 'cool_off')
             │
             ▼
        daily cap hit? ── yes ─────────► STOP (pick up tomorrow)
             │
             ▼
       POST /branded_content_ad_permissions
             │
       ┌─────┴──────┐
       │ 200        │ 4xx
       ▼            ▼
    RECORD       RECORD
    PENDING      <error>
```

---

## 5. What gets recorded

### Local SQLite — `partnership_status_local.sqlite`

```
   reachout
   ─────────────────────────────────────────────────────────────
   handle           lowercased @-stripped Instagram username (PK)
   permission_id    Meta's invite identifier
   status           PENDING_APPROVAL | APPROVED | REJECTED |
                    REVOKED | not_on_ig | not_eligible | bad_url |
                    cool_off | err_<code>
   first_seen_at    ISO timestamp — first encounter
   last_changed_at  ISO timestamp — last status transition
   last_checked_at  ISO timestamp — last Meta API ping
   note             source post_link + any error text
```

### Optional write-back to `saadaa-creatorhub` (if columns exist)

```
   UPDATE <source_table>
      SET partnership_invite_sent_at = NOW(),
          partnership_permission_id  = <Meta id>,
          partnership_status         = 'PENDING_APPROVAL'
    WHERE username = <HANDLE>;
```

### Audit log file — `logs/auto_invite_partnership.log`

```
   2026-06-27T12:34:01Z  send       @neha_styles      → permission_id=78214…  status=PENDING_APPROVAL
   2026-06-27T12:34:03Z  skip       @brand_x          → reason=already_pending
   2026-06-27T12:34:04Z  not_on_ig  @fake_handle_42   → reason=user_not_found
   2026-06-27T12:34:06Z  cooldown   X-App-Usage=83%   → sleep 300s
```

Token scrub regex applied to every line so credentials never leak to logs.

---

## 6. Safety rails

```
   ▸  Dry-run is the DEFAULT — script never sends invites unless --apply
      is passed explicitly.
   ▸  --limit N      caps invites in a single run (default 50)
   ▸  Daily cap 200  hard ceiling across runs (configurable)
   ▸  Excluded list  partnership_excluded_handles.txt — never invite these
   ▸  Cool-off 30 d  no re-invite of a REJECTED handle within 30 days
   ▸  Idempotent     never re-invite if pending or approved already exists
   ▸  Rate limit     parse X-App-Usage header, cool 300s at ≥75% any bucket
   ▸  Token scrub    every log line passes through a redaction regex
   ▸  Audit trail    every send is persisted with timestamp + outcome
```

---

## 7. APIs in play

```
   META  Graph API v22.0 (api.facebook.com)
   ─────────────────────────────────────────────────────────────────────
   GET   /17841412619002528?fields=business_discovery.username(X){id}
            → verify the handle exists on IG

   GET   /17841412619002528/branded_content_ad_permissions?creator_username=X
            → check existing invite status

   POST  /17841412619002528/branded_content_ad_permissions
   body    creator_instagram_username=X
            → send the invite (returns permission_id)


   SUPABASE  (saadaa-creatorhub, REST/v1)
   ─────────────────────────────────────────────────────────────────────
   GET   /rest/v1/<source_table>
            ?select=username,post_link,...
            &post_link=not.is.null
            &invite_sent_at=is.null     (if column exists)
            &limit=50

   PATCH /rest/v1/<source_table>?username=eq.X
            → optional write-back of invite status
```

---

## 8. Credentials needed

| Env variable               | Purpose                                              |
|----------------------------|------------------------------------------------------|
| `CREATOR_HUB_URL`          | Supabase REST base of saadaa-creatorhub              |
| `CREATOR_HUB_SERVICE_ROLE` | service-role key (anon is blocked by RLS)            |
| `META_ACCESS_TOKEN`        | Long-lived Meta token with the scopes below          |

Meta token scopes required:
```
   instagram_branded_content_ads_brand
   instagram_branded_content_creator
   pages_show_list
   pages_read_engagement
   business_management
```

---

## 9. Ops cheatsheet

```
   # safe dry-run on a single creator
   python auto_invite_partnership.py --handle neha_styles --dry-run

   # dry-run on all eligible rows
   python auto_invite_partnership.py --dry-run --limit 50

   # actually send (gated by daily cap + idempotency)
   python auto_invite_partnership.py --apply --limit 50

   # refresh status of pending invites (no new sends)
   python auto_invite_partnership.py --refresh-pending

   # schedule it — e.g. every 15 minutes via Windows Task Scheduler
   #   action  : python.exe D:\Creative_Testing_Dashboard\backend\auto_invite_partnership.py --apply --limit 25
   #   trigger : every 15 minutes
```

---

## 10. What's not yet wired (blocks the build)

```
   [ ] CREATOR_HUB_SERVICE_ROLE added to .env
        (current anon key returns 0 rows under RLS)

   [ ] Confirm which table holds post_link
        candidates: cleaned_data · creators · ig_data_historic

   [ ] Confirm the handle column name in that table
        candidates: username · ig_handle · instagram_handle

   [ ] (Optional) write-back columns on the source table
        partnership_invite_sent_at · partnership_permission_id ·
        partnership_status
```

Once those land, the orchestrator script `auto_invite_partnership.py` is a
straight implementation of the four gates documented above — roughly 150
lines including logging and SQLite plumbing.

---

*Last updated 2026-06-27 · `backend/AUTO_INVITE_FLOW.md`*
