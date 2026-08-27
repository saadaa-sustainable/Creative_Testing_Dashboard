import { BrowserRouter, Routes, Route } from "react-router-dom";
import { Sidebar } from "../components/Sidebar";
import { Home } from "../sections/Home";
import { Placeholder } from "../sections/Placeholder";
import { SECTIONS } from "./sections";

export function AppRouter() {
  return (
    <BrowserRouter>
      <div className="app-shell">
        <Sidebar />
        <main className="app-main">
          <Routes>
            <Route path="/" element={<Home />} />
            {SECTIONS.filter((s) => s.slug !== "").map((s) => (
              <Route
                key={s.slug}
                path={`/${s.slug}`}
                element={<Placeholder />}
              />
            ))}
            <Route path="*" element={<Placeholder />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
