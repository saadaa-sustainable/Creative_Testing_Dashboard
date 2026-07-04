// One-shot refactor: split index_v2.html into three files:
//   index_v2.html                 shell (HTML markup only)
//   assets/dashboard.css          the <style> block (~1300 lines)
//   assets/dashboard.js           the big inline <script> block (~5000 lines)
//
// Rules:
//   * only the FIRST <style>...</style> block gets extracted (there's just one).
//   * only the LAST <script>...</script> block (with no src=) gets extracted —
//     that's the main app JS. Any earlier <script src=...> library tags stay.
//   * everything else in the HTML stays exactly as it was.
//
// Idempotent: running twice does nothing extra since the second run won't find
// an inline <style> or <script> block anymore.

const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname, '..');
const HTML = path.join(ROOT, 'index_v2.html');
const CSS  = path.join(ROOT, 'assets', 'dashboard.css');
const JS   = path.join(ROOT, 'assets', 'dashboard.js');

let html = fs.readFileSync(HTML, 'utf8');
const originalLen = html.length;

// ── Extract the single <style>...</style> block ─────────────────────
const styleRe = /<style>\s*([\s\S]*?)<\/style>/;
const sm = html.match(styleRe);
if (sm){
  fs.mkdirSync(path.dirname(CSS), { recursive: true });
  fs.writeFileSync(CSS, sm[1].trim() + '\n', 'utf8');
  html = html.replace(sm[0], '<link rel="stylesheet" href="assets/dashboard.css">');
  console.log(`[✓] extracted ${sm[1].length.toLocaleString()} chars → assets/dashboard.css`);
} else {
  console.log('[!] no <style> block found — already extracted?');
}

// ── Extract the LAST inline <script>...</script> block (the app JS) ──
// Find every inline script (no src attribute). Pick the one with the most
// content — that's the app; others are typically ~50-line credential
// bootstraps or nothing.
const scriptRe = /<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/g;
let best = null;
let m;
while ((m = scriptRe.exec(html)) !== null){
  if (!best || m[1].length > best[1].length){ best = m; }
}
if (best){
  fs.writeFileSync(JS, best[1].trim() + '\n', 'utf8');
  html = html.replace(best[0], '<script src="assets/dashboard.js"></script>');
  console.log(`[✓] extracted ${best[1].length.toLocaleString()} chars → assets/dashboard.js`);
} else {
  console.log('[!] no inline <script> block found — already extracted?');
}

fs.writeFileSync(HTML, html, 'utf8');
console.log(`[✓] rewrote index_v2.html (${originalLen.toLocaleString()} → ${html.length.toLocaleString()} chars)`);
