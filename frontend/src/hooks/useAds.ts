import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "../api/client";
import type { AdsResponse } from "../types/ads";

/**
 * Fetch the ~15-18k row ae_table_view via GET /api/ads.
 * Server-side TTL cache = 15min, browser cache = 5min, TanStack Query
 * cache = 15min — so switching between sections that share this data
 * (AE, Creative Testing, Ad Intelligence, Untested) fires zero refetches.
 */
export function useAds(params?: { status?: string; since?: string }) {
  const status = params?.status ?? null;
  const since = params?.since ?? "2025-01-01";
  const qs = new URLSearchParams();
  if (status) qs.set("status", status);
  qs.set("since", since);

  return useQuery<AdsResponse, Error>({
    queryKey: ["ads", { status, since }],
    queryFn: () => apiFetch<AdsResponse>(`/api/ads?${qs.toString()}`),
    staleTime: 15 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    refetchOnWindowFocus: false,
  });
}
