// FILE: web/static/js/campaign_templates/main.js
// DATE: 2026-01-18
// PURPOSE: Минимальный JS: клики по кнопкам overlays -> fetch CSS из API -> yyTplApplyOverlay().
// CHANGE: Никакой отрисовки кнопок в JS.

(function () {
  "use strict";

  async function fetchText(url) {
    const r = await fetch(url, { credentials: "same-origin" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.text();
  }

  async function onOverlayClick(ev) {
    const btn = ev?.target?.closest?.(".yy-tpl-overlay-btn");
    if (!btn) return;

    const gid = String(btn.dataset.gid || "").trim();
    const type = String(btn.dataset.type || "").trim(); // colors|fonts
    const name = String(btn.dataset.name || "").trim();

    if (!(gid && (type === "colors" || type === "fonts") && name)) return;

    const url =
      `/panel/campaigns/templates/_global-style-css/?gid=${encodeURIComponent(gid)}` +
      `&type=${encodeURIComponent(type)}&name=${encodeURIComponent(name)}`;

    try {
      const css = await fetchText(url);
      if (typeof window.yyTplApplyOverlay === "function") window.yyTplApplyOverlay(type, css || "");
    } catch (_) {}
  }

  function init() {
    document.addEventListener("click", onOverlayClick, true);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
