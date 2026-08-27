# Creative Testing Dashboard — React Frontend

New React + TypeScript frontend, being ported section-by-section from the vanilla-JS `assets/dashboard.js`.

**Status:** scaffold + routing + API client + Home page. All other sections show a Placeholder that links to the vanilla dashboard.

## Stack

- **Vite** — dev server + build
- **React 19 + TypeScript** — UI
- **React Router 7** — client-side routing (`/ads-analyse`, `/landing-page`, etc.)
- **TanStack Query 5** — data fetching + cache layer (mirrors FastAPI's 15-min TTL)

## Directory layout

```
frontend/
├── src/
│   ├── api/          # fetch wrapper + typed responses
│   │   └── client.ts
│   ├── hooks/        # useQuery-based data hooks per endpoint
│   │   └── useAds.ts
│   ├── types/        # shared TypeScript types
│   │   └── ads.ts
│   ├── router/
│   │   ├── AppRouter.tsx   # top-level route table
│   │   └── sections.ts     # section registry (slug ↔ title ↔ status)
│   ├── components/
│   │   └── Sidebar.tsx     # persistent nav
│   ├── sections/           # ONE file per view
│   │   ├── Home.tsx        # ready
│   │   └── Placeholder.tsx # coming-soon shim
│   ├── main.tsx
│   └── index.css
├── vite.config.ts    # proxies /api/* to FastAPI on :8000 during dev
├── package.json
└── tsconfig.*.json
```

## Dev workflow

**Terminal 1** — FastAPI backend on port 8000:
```bash
cd backend
python -m uvicorn api_ae:app --host 127.0.0.1 --port 8000 --reload
```

**Terminal 2** — Vite dev server on port 5173:
```bash
cd frontend
npm run dev
```

Open **http://localhost:5173** — dev server proxies `/api/*` + `/rest/v1/*` to the FastAPI backend, so cross-origin issues never arise. Hot module reload works on file save.

## Production build

```bash
cd frontend
npm run build
```

Emits to `../dist/` (repo-root `dist/` folder). To have FastAPI serve the React bundle instead of the vanilla `index_v2.html`, update `backend/api_ae.py`:
```python
_ROOT_HTML = _pl.Path(__file__).parent.parent / "dist" / "index.html"
_ASSETS_DIR = _pl.Path(__file__).parent.parent / "dist" / "assets"
```
Once ALL sections are ported. Until then, keep the vanilla path live and only switch specific section routes over.

## Porting checklist for a new section

1. Read `assets/dashboard.js` — find `showView(v)` where `v === 'x'` and every fetch/render for that view
2. Add types to `src/types/x.ts` for any new response shapes
3. Add data hook to `src/hooks/useX.ts` — one `useQuery` per endpoint
4. Add component to `src/sections/X.tsx`
5. Update `src/router/sections.ts` — flip status from `"coming-soon"` → `"ready"`
6. Update `src/router/AppRouter.tsx` — replace `<Placeholder />` with `<X />`
7. Verify — visit `http://localhost:5173/{slug}` — should render without proxy errors

## API contract

The FastAPI backend serves:

| Endpoint | Cache TTL | Purpose |
|---|---|---|
| `GET /api/ads?status=&since=` | 15min | ae_table_view — 15-18k rows |
| `GET /api/window_metrics?from=&to=` | 15min | per-ad metrics for date window |
| `GET /api/window_shopify?from=&to=` | 15min | shopify orders + sales per ad in window |
| `GET /api/delivery?from=&to=` | 15min | list of ad_ids that delivered in window |
| `GET /rest/v1/{table}` | none | PostgREST-compatible passthrough |
| `POST /rest/v1/rpc/{fn}` | none | RPC passthrough |
| `GET /api/cache/stats` | never cached | hit rate + entries |
| `POST /api/cache/invalidate?prefix=` | never cached | manual flush |

`X-Cache: MISS|HIT` header on cached endpoints for observability.

Every response also carries `Cache-Control: public, max-age=300, stale-while-revalidate=60` so the browser also caches independently of TanStack Query.

## Next sessions

Suggested order (simplest → most complex):
1. **Landing Page** — one table, one chart, ~500 rows. Good validation of the port pattern.
2. **Incremental Analysis** — table + saturation chart. Tests our chart-library choice.
3. **Untested Assets** — small list, useful for validating filter UX.
4. **Creative Testing** — moderate complexity, category KPIs + list.
5. **Ads Analyse** — the beast. 18k-row virtualized table, multi-filter, drawer, historic mode. Save for last.
6. **Ad Intelligence** — inspector drawer over AE. Reuses AE data.
7. **Creative Lifecycle** — frequency-bucket view.
8. **CPI Inspector** — SKU-level costs.
9. **Historic Reach / Historic Untested** — historic-mode variants of existing views.

Do NOT try to port more than one section per session. Each section is 500-2000 lines of vanilla JS and 200-500 lines of React equivalent.
