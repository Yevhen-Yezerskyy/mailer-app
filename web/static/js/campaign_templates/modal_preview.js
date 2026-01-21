// FILE: web/static/js/campaign_templates/modal_preview.js
// DATE: 2026-01-21
// PURPOSE: Переключение вкладок превью-модалки (VIEW / HTML / HTML EMAIL).
// CHANGE: Делегирование кликов (работает для HTML-фрагментов, вставленных динамически).

(function () {
  "use strict";

  function showPane(root, key) {
    if (!root || !key) return;
    const panes = root.querySelectorAll(".yyPrevPane[data-pane]");
    panes.forEach(function (p) {
      if ((p.getAttribute("data-pane") || "").trim() === key) p.classList.remove("hidden");
      else p.classList.add("hidden");
    });
  }

  document.addEventListener("click", function (e) {
    const btn = e.target && e.target.closest ? e.target.closest(".yyPrevTab[data-tab]") : null;
    if (!btn) return;

    const root = btn.closest(".yy-preview-root");
    if (!root) return;

    const key = (btn.getAttribute("data-tab") || "").trim();
    if (!key) return;

    e.preventDefault();
    showPane(root, key);
  });

  // дефолт: если модалка уже в DOM на момент загрузки скрипта
  document.addEventListener("DOMContentLoaded", function () {
    document.querySelectorAll(".yy-preview-root").forEach(function (root) {
      showPane(root, "view");
    });
  });
})();
