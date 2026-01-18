// FILE: web/static/js/campaign_templates/main.js
// DATE: 2026-01-18
// PURPOSE: Клики по overlays + выбор GlobalTemplate (gl_tpl) с confirm и redirect.
// CHANGE: Добавлен обработчик .yy-tpl-global-btn (confirm -> ?gl_tpl=<gid>).

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

  function onGlobalTemplateClick(ev) {
    const btn = ev?.target?.closest?.(".yy-tpl-global-btn");
    if (!btn) return;

    const gid = String(btn.dataset.gid || "").trim();
    if (!(gid && /^\d+$/.test(gid))) return;

    const ok = window.confirm(
      "Вы уверены? Текущая работа над шаблоном будет частично или полностью потеряна."
    );
    if (!ok) return;

    try {
      const u = new URL(window.location.href);
      const q = u.searchParams;
      q.set("gl_tpl", gid);
      window.location.search = "?" + q.toString();
    } catch (_) {
      // fallback
      window.location.href = "?gl_tpl=" + encodeURIComponent(gid);
    }
  }

  function init() {
    document.addEventListener("click", onOverlayClick, true);
    document.addEventListener("click", onGlobalTemplateClick, true);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
