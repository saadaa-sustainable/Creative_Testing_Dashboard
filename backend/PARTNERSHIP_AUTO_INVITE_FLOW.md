# Partnership Ads — Auto-Invite Flow

End-to-end design for sending Meta Partnership Ad invites automatically
when a creator's `post_link` becomes available in `saadaa-creatorhub`.

---

## 1 · Trigger

A creator becomes eligible for an auto-invite the moment **`post_link` is non-null**
in the `saadaa-creatorhub` Supabase project (table TBD; candidates listed below).

```
saadaa-creatorhub                          ▸  trigger
  ├─ cleaned_data       (RLS-locked at probe)
  ├─ creators           (RLS-locked at probe)
  ├─ ig_data_historic   (RLS-locked at probe — needs service-role key)
  └─ … (whichever table is canonical for "post_link → @handle" mapping)
```

> **Prerequisite to wire this up**: the `.env` needs a service-role key for the
> creatorhub project (current `CREATOR_HUB_ACCESS` is the anon key and returns
> zero rows under RLS).

---

## 2 · Credentials required

| Env variable               | Purpose                                              | Required scopes                                       |
|----------------------------|------------------------------------------------------|-------------------------------------------------------|
| `CREATOR_HUB_URL`          | REST base of saadaa-creatorhub                       | —                                                     |
| `CREATOR_HUB_SERVICE_ROLE` | reads `post_link` rows under RLS                     | service_role                                          |
| `META_ACCESS_TOKEN`        | Meta Graph API token for the Saadaa brand           | `instagram_branded_content_ads_brand`                |
|                            |                                                      | `instagram_branded_content_creator`                   |
|                            |                                                      | `pages_show_list`                                     |
|                            |                                                      | `pages_read_engagement`                               |
|                            |                                                      | `business_management`                                 |

Brand IGBA used as the issuer of the invite:
`SRC = 17841412619002528` (Saadaa `@saadaadesigns`).

---

## 3 · Step-by-step flow

```
                ┌────────────────────────────────────┐
                │ saadaa-creatorhub                  │
                │ rows with post_link IS NOT NULL    │
                │ AND invite_sent_at IS NULL         │
                └─────────────────┬──────────────────┘
                                  │
                ┌─────────────────▼──────────────────┐
                │ extract_handle(post_link)          │
                │  parses @username from the URL     │
                │  https://instagram.com/<HANDLE>/…  │
                └─────────────────┬──────────────────┘
                                  │
                ┌─────────────────▼──────────────────┐
                │ optional: Business Discovery probe │
                │ GET /17841412619002528             │
                │     ?fields=business_discovery     │
                │       .username(<HANDLE>){id}      │
                │ Skip silently if user not found.   │
                └─────────────────┬──────────────────┘
                                  │
                ┌─────────────────▼──────────────────┐
                │ idempotency check                  │
                │ GET /17841412619002528             │
                │     /branded_content_ad_permissions│
                │     ?creator_username=<HANDLE>     │
                │ If existing status is              │
                │   PENDING_APPROVAL or APPROVED     │
                │ → SKIP, write reason to local DB.  │
                └─────────────────┬──────────────────┘
                                  │
                ┌─────────────────▼──────────────────┐
                │ send invite                        │
                │ POST /17841412619002528            │
                │     /branded_content_ad_permissions│
                │ body: {                            │
                │   creator_instagram_username:      │
                │     <HANDLE>,                      │
                │   access_token: META_ACCESS_TOKEN  │
                │ }                                  │
                │ Response: {id: <permission_id>}    │
                └─────────────────┬──────────────────┘
                                  │
                ┌─────────────────▼──────────────────┐
                │ persist outcome                    │
                │ ── partnership_status_local.sqlite │
                │     (existing schema)              │
                │       handle, permission_id,       │
                │       status, first_seen_at,       │
                │       last_changed_at,             │
                │       last_checked_at, note        │
                │ ── (optional) UPDATE the source    │
                │     creatorhub row with            │
                │     invite_sent_at + permission_id │
                └─────────────────┬──────────────────┘
                                  │
                ┌─────────────────▼──────────────────┐
                │ rate-limit / cooldown              │
                │ • baseline sleep 1.5s between rows │
                │ • parse X-App-Usage header         │
                │ • sleep 300s if any bucket ≥75%    │
                │ • back-off 5/15/45s on 5xx + 429   │
                └────────────────────────────────────┘
```

---

## 4 · API references

### Meta endpoints

```
POST   /v22.0/17841412619002528/branded_content_ad_permissions
       body: creator_instagram_username=<handle>&access_token=<META_TOKEN>
       returns: {"id": "<permission_id>"}
       errors: 100=missing field, 200=permission, 4=throttle, 17=rate limit

GET    /v22.0/17841412619002528/branded_content_ad_permissions
       ?creator_username=<handle>&access_token=<META_TOKEN>
       returns: {"data": [ {id, status, creator_user, ...} ]}
       status enum: PENDING_APPROVAL · APPROVED · REJECTED · REVOKED

GET    /v22.0/17841412619002528
       ?fields=business_discovery.username(<handle>){username,id,followers_count}
       returns: {"business_discovery": {...}}  or  error 24=user not found
```

### Creatorhub queries

```
GET  {CH_URL}/rest/v1/<table>
     ?select=<handle_col>,post_link,...
     &post_link=not.is.null
     &invite_sent_at=is.null         (if column exists)
     &order=created_at.desc
     &limit=50

Header: apikey + Authorization: Bearer <SERVICE_ROLE_KEY>
```

---

## 5 · Run modes

| Mode        | Behaviour                                                          |
|-------------|--------------------------------------------------------------------|
| `--dry-run` | prints every candidate handle + would-be API body, **no POST**      |
| (default)   | dry-run, single batch                                              |
| `--apply`   | actually POSTs invites                                             |
| `--limit N` | cap to N invites per run                                           |
| `--handle X`| single-handle test mode (skips DB read, sends one invite)          |
| `--reseed`  | clear local "skip because already invited" cache                   |

---

## 6 · Safety rails

1. **Dry-run default** — script never sends an invite unless `--apply` is passed.
2. **Idempotency lock** — never POST if existing permission status is
   `PENDING_APPROVAL` or `APPROVED`. Verified via GET before each send.
3. **Cool-off window** — if a handle was invited and rejected within the last
   30 days, do not re-invite (configurable).
4. **Exclusion list** — `partnership_excluded_handles.txt` of handles to skip
   permanently (e.g., competitors, blocked).
5. **Daily cap** — soft limit of 200 invites/day to stay well below Meta's
   account-level limits and avoid being flagged for spam.
6. **Token leak scrub** — every log line passes through the regex scrub
   `(EAA[\w]{30,}|IGQ[\w-]{20,}|eyJ[\w.\-]{40,})` before stdout/file write
   (same pattern `partnership_status_ui.py` already uses).
7. **Audit trail** — every send writes a row to
   `logs/auto_invite_partnership.log` with timestamp, handle, response code,
   permission_id (no token).

---

## 7 · Failure modes & handling

| Symptom                                          | Cause                            | Action                                                |
|--------------------------------------------------|----------------------------------|-------------------------------------------------------|
| HTTP 401, error code 190                         | Token expired                    | Stop run, log; require manual token refresh           |
| HTTP 400, code 100 "missing field"               | Bad request shape                | Log + fix script — likely API version drift           |
| HTTP 400, code 24 "user not found"               | Handle doesn't exist on IG       | Mark `not_on_ig`, skip                                |
| HTTP 400, code 270 "creator not eligible"        | Account not allowed (private, business-only restriction, region block, etc.) | Mark `not_eligible`, skip          |
| HTTP 200, response includes `error`              | Often a soft validation error    | Log full body, continue                               |
| HTTP 429 / X-App-Usage ≥ 95                      | Throttled                        | Sleep 5 → 15 → 45 → 300s, retry                       |
| HTTP 500/502/503                                 | Meta side                        | Retry with exponential back-off                       |

---

## 8 · Persistence schema

Local SQLite — `partnership_status_local.sqlite` (already exists; same shape as
`partnership_status_ui.py`):

```sql
CREATE TABLE reachout (
  handle           TEXT PRIMARY KEY,    -- always stored lowercase
  permission_id    TEXT,                -- Meta's invite id
  status           TEXT,                -- PENDING_APPROVAL / APPROVED / REJECTED / REVOKED / not_on_ig / not_eligible
  first_seen_at    TEXT,                -- ISO timestamp, first time we saw the handle
  last_changed_at  TEXT,                -- ISO timestamp, last status transition
  last_checked_at  TEXT,                -- ISO timestamp, last GET to Meta
  note             TEXT                 -- free-form note: source post URL, error text, etc.
);
```

Optional write-back to creatorhub (if a `partnership_invite` column exists on
the source table):

```sql
UPDATE <source_table>
   SET partnership_invite_sent_at = NOW(),
       partnership_permission_id  = <id from Meta>,
       partnership_status         = 'PENDING_APPROVAL'
 WHERE username = <handle>;
```

---

## 9 · Files involved

```
backend/
  PARTNERSHIP_AUTO_INVITE_FLOW.md      ← this document
  partnership_status_ui.py             ← existing Flask UI (manual invite path)
  _probe_partnership_token.py          ← existing token-scope diagnostic
  _probe_creatorhub_postlink.py        ← new: validates source table & RLS
  auto_invite_partnership.py           ← (to build) the orchestrator
  partnership_status_local.sqlite      ← existing local tracking DB
  logs/
    auto_invite_partnership.log        ← (to create) audit trail
```

---

## 10 · What needs to land before we can build the orchestrator

1. **Service-role key** for the saadaa-creatorhub project added to `.env`
   (any var name; recommended `CREATOR_HUB_SERVICE_ROLE`).
2. **Confirm canonical source table** — which of `cleaned_data` / `creators`
   / `ig_data_historic` holds the `post_link` column. Anon key returns 0 rows
   so we couldn't introspect.
3. **Confirm handle column name** in that table (e.g. `username`, `ig_handle`,
   `instagram_handle`).
4. **Optional: write-back columns** in that table if you want the invite
   status tracked back into creatorhub (otherwise it lives only in the local
   SQLite).
5. **Token scope verification** — run `_probe_partnership_token.py` once
   with the current `META_ACCESS_TOKEN` to confirm
   `instagram_branded_content_ads_brand` is granted.

Once those four items are confirmed, the orchestrator is a ~150-line script.
