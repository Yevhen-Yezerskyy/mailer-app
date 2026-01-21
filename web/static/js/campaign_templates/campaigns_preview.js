// FILE: web/static/js/campaign_templates/campaigns_preview.js
// DATE: 2026-01-21
// PURPOSE: Preview кампаний (нижняя таблица): открыть модалку по id через YYModal (GET).
// CHANGE: Вызывает /panel/campaigns/campaigns/preview/modal/?id=...

(function () {
  "use strict";

  function previewById(id) {
    if (!window.YYModal || typeof window.YYModal.open !== "function") return;
    const v = String(id || "").trim();
    if (!v) return;
    window.YYModal.open(`url=/panel/campaigns/campaigns/preview/modal/?id=${encodeURIComponent(v)}`);
  }

  window.yyCampPreviewById = previewById;
})();
