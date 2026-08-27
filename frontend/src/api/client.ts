/**
 * FastAPI client wrapper. During development, Vite proxies /api/* to
 * http://127.0.0.1:8000 (see vite.config.ts). In production, the React
 * bundle is served BY the FastAPI backend, so same-origin — no CORS,
 * no explicit base URL needed.
 */

const _isDev = import.meta.env.DEV;
export const API_BASE = _isDev ? "" : ""; // both cases: same-origin/proxied

export class ApiError extends Error {
  status: number;
  url: string;
  constructor(status: number, url: string, message: string) {
    super(message);
    this.status = status;
    this.url = url;
    this.name = "ApiError";
  }
}

export async function apiFetch<T>(
  path: string,
  init?: RequestInit,
): Promise<T> {
  const url = API_BASE + path;
  const t0 = performance.now();
  const resp = await fetch(url, init);
  if (!resp.ok) {
    const body = await resp.text().catch(() => "");
    throw new ApiError(resp.status, url, `${resp.status}: ${body.slice(0, 200)}`);
  }
  const data = (await resp.json()) as T;
  if (_isDev) {
    const ms = performance.now() - t0;
    const xCache = resp.headers.get("x-cache") ?? "-";
    // eslint-disable-next-line no-console
    console.debug(`[api] ${path} (${xCache}) ${ms.toFixed(0)}ms`);
  }
  return data;
}
