import { NavLink } from "react-router-dom";
import { SECTIONS } from "../router/sections";

export function Sidebar() {
  return (
    <aside className="sidebar">
      <div className="sidebar-brand">
        <div className="brand-logo">CT</div>
        <div className="brand-text">
          <div className="brand-name">Creative Testing</div>
          <div className="brand-sub">saadaa.in</div>
        </div>
      </div>
      <nav className="sidebar-nav">
        {SECTIONS.map((s) => (
          <NavLink
            key={s.slug}
            to={s.slug === "" ? "/" : `/${s.slug}`}
            end={s.slug === ""}
            className={({ isActive }) =>
              `sidebar-item ${isActive ? "is-active" : ""} ${
                s.status === "coming-soon" ? "is-soon" : ""
              }`
            }
          >
            <span className="sidebar-item-title">{s.title}</span>
            {s.status === "coming-soon" && (
              <span className="sidebar-item-badge">soon</span>
            )}
          </NavLink>
        ))}
      </nav>
    </aside>
  );
}
