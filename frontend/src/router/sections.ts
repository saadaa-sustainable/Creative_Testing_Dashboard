/**
 * Section registry — the single source of truth for URL slug ↔ display
 * name ↔ short description. Sidebar, home page, and route table all
 * read from here. Kept in sync with backend/api_ae.py's SECTION_SLUG_TO_VIEW.
 */
export interface Section {
  slug: string;                 // URL segment, e.g. "ads-analyse"
  title: string;                // sidebar label
  description: string;          // home-card blurb
  status: "ready" | "coming-soon";  // is this section ported to React yet?
}

export const SECTIONS: Section[] = [
  { slug: "",                      title: "Home",                  description: "Landing page — jump to any section",           status: "ready" },
  { slug: "ads-analyse",           title: "Ads Analyse",            description: "Ad-level performance table + KPI cards",       status: "coming-soon" },
  { slug: "ad-intelligence",       title: "Ad Intelligence",        description: "Right-side inspector with metrics + filters",  status: "coming-soon" },
  { slug: "incremental-analysis",  title: "Incremental Analysis",   description: "Baseline-anchored incremental reach model",    status: "coming-soon" },
  { slug: "historic-reach",        title: "Historic Reach",         description: "Reach & frequency across historic window",     status: "coming-soon" },
  { slug: "creative-testing",      title: "Creative Testing",       description: "Testing category funnel + winners view",       status: "coming-soon" },
  { slug: "creative-lifecycle",    title: "Creative Lifecycle",     description: "Frequency-bucket lifecycle stages",            status: "coming-soon" },
  { slug: "landing-page",          title: "Landing Page Analysis",  description: "Sessions × ads landing-page rollup",           status: "coming-soon" },
  { slug: "untested-assets",       title: "Untested Assets",        description: "Assets ready to test — untried creatives",     status: "coming-soon" },
  { slug: "historic-untested",     title: "Historic Untested",      description: "Historic-cutoff untested assets",              status: "coming-soon" },
  { slug: "cpi-inspector",         title: "CPI Inspector",          description: "Cost-per-installation by SKU + demand model",  status: "coming-soon" },
  { slug: "inventory",             title: "Inventory",              description: "SKU-level inventory + shipping",               status: "coming-soon" },
];

export const SECTION_BY_SLUG: Record<string, Section> = Object.fromEntries(
  SECTIONS.map((s) => [s.slug, s]),
);
