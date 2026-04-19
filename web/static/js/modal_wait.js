// FILE: web/static/js/modal_wait.js
// DATE: 2026-03-14
// PURPOSE: Global non-closable wait overlay with the same backdrop style as modal.

(function () {
  const i18n = window.yyI18n || (document.documentElement && document.documentElement.yyI18n) || {};
  const t = (key, fallback) => {
    const v = i18n[key];
    return typeof v === "string" && v.trim() ? v : fallback;
  };
  const escAttr = (value) => String(value || "").replace(/"/g, "&quot;");

  function ensureRoot() {
    let el = document.getElementById("yy-wait-loading");
    if (el) return el;

    el = document.createElement("div");
    el.id = "yy-wait-loading";
    el.className = "yy-loading hidden";
    el.setAttribute("aria-hidden", "true");
    const loadingLabel = escAttr(t("loading_label", "Loading"));
    el.innerHTML =
      '<div class="yy-loading__backdrop" style="background: rgba(5, 112, 235, 0.012);"></div>' +
      '<div class="yy-loading__box" role="status" aria-live="polite" aria-label="' + loadingLabel + '">' +
        '<img src="/static/img/spinner.svg" alt="" class="w-[150px] h-[150px]">' +
      '</div>';
    document.body.appendChild(el);
    return el;
  }

  function open() {
    const el = ensureRoot();
    el.classList.remove("hidden");
    el.setAttribute("aria-hidden", "false");
  }

  function close() {
    const el = ensureRoot();
    if (!el) return;
    el.classList.add("hidden");
    el.setAttribute("aria-hidden", "true");
  }

  window.YYWaitModal = { open, close };
})();
