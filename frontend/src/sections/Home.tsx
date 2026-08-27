import { Link } from "react-router-dom";
import { SECTIONS } from "../router/sections";
import { useAds } from "../hooks/useAds";

/**
 * Home — grid of section cards. Prefetches /api/ads so any downstream
 * ads-consuming section (AE, Testing, AdIntel, Untested) is instant.
 * The `useAds` call here shares its cache with those sections thanks
 * to TanStack Query.
 */
export function Home() {
  const { data, isPending, isError, error } = useAds();

  return (
    <div className="section">
      <header className="section-header">
        <h1 className="section-title">Creative Testing Dashboard</h1>
        <p className="section-subtitle">
          {isPending && "Prefetching ads data …"}
          {isError && (
            <span className="err">API error: {error.message}</span>
          )}
          {data && (
            <>
              <strong>{data.count.toLocaleString()}</strong> ads loaded and
              cached. Click any section below to dive in.
            </>
          )}
        </p>
      </header>

      <div className="home-grid">
        {SECTIONS.filter((s) => s.slug !== "").map((s) => (
          <Link
            key={s.slug}
            to={`/${s.slug}`}
            className={`home-card ${s.status === "coming-soon" ? "is-soon" : ""}`}
          >
            <div className="home-card-title">{s.title}</div>
            <div className="home-card-desc">{s.description}</div>
            <div className="home-card-meta">
              {s.status === "coming-soon" ? "Coming soon" : "Ready"}
              <span className="home-card-arrow">→</span>
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}
