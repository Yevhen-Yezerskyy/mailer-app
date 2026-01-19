// FILE: web/static/js/campaign_letters/preview.js
// DATE: 2026-01-19
// PURPOSE: Preview письма кампании через YYModal (POST from editor) или GET by id (из таблицы кампаний не нужно).
// CHANGE: (new) endpoint /panel/campaigns/campaigns/preview/modal-from-editor/.

(function () {
  "use strict";

  const $ = (s) => document.querySelector(s);

  function normalizeTabsTo2Spaces(s) {
    return (s || "").replace(/\t/g, "  ");
  }

  function getMode() {
    const el = $("#yyEditorMode");
    const v = el ? String(el.value || "").trim() : "user";
    return v === "advanced" ? "advanced" : "user";
  }

  function getCampaignId() {
    const el = $("#yyCampaignId");
    return el ? String(el.value || "").trim() : "";
  }

  function getUserHtml() {
    try {
      return (window.YYCampaignLetterTiny && window.YYCampaignLetterTiny.getHtml)
        ? (window.YYCampaignLetterTiny.getHtml() || "")
        : "";
    } catch (_) {
      return "";
    }
  }

  function getAdvHtml() {
    try {
      if (typeof window.yyCampEnsureCodeMirror === "function") window.yyCampEnsureCodeMirror();
      return (typeof window.yyCampAdvGetHtml === "function") ? (window.yyCampAdvGetHtml() || "") : "";
    } catch (_) {
      return "";
    }
  }

  function previewCurrent() {
    if (!window.YYModal || typeof window.YYModal.open !== "function") return;

    const id = getCampaignId();
    if (!id) return;

    const mode = getMode();
    const html = mode === "advanced" ? normalizeTabsTo2Spaces(getAdvHtml()) : (getUserHtml() || "");
    window.YYModal.open("post=/panel/campaigns/campaigns/preview/modal-from-editor/", {
      id: id,
      editor_html: html || "",
    });
  }

  window.yyCampPreviewCurrent = previewCurrent;
})();
