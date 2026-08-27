import { useParams, Link } from "react-router-dom";
import { SECTION_BY_SLUG } from "../router/sections";

/**
 * Placeholder for sections that haven't been ported to React yet.
 * Shows the section title + points to the vanilla-JS URL as a fallback
 * so the user can still access the feature while the React port lands.
 */
export function Placeholder() {
  const { slug } = useParams();
  const section = slug ? SECTION_BY_SLUG[slug] : undefined;

  if (!section) {
    return (
      <div className="section">
        <h1 className="section-title">404 — unknown section</h1>
        <Link to="/">Back home</Link>
      </div>
    );
  }

  return (
    <div className="section">
      <header className="section-header">
        <h1 className="section-title">{section.title}</h1>
        <p className="section-subtitle">{section.description}</p>
      </header>
      <div className="empty-state">
        <div className="empty-state-title">Coming soon</div>
        <p>
          This section hasn't been ported to React yet. In the meantime,
          use the existing vanilla-JS dashboard:
        </p>
        <a
          className="empty-state-link"
          href={`/../index_v2.html?apiBase=${window.location.origin}`}
        >
          Open vanilla dashboard →
        </a>
        <div className="empty-state-hint">
          Ported sections will land here progressively. Check{" "}
          <code>src/sections/</code> for the current list.
        </div>
      </div>
    </div>
  );
}
