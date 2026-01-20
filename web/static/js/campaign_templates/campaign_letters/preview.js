// FILE: web/static/js/campaign_templates/campaign_letters/preview.js
// DATE: 2026-01-20
// PURPOSE: Preview: никаких extract на клиенте. Шлем {id, editor_mode, editor_html}.
// CHANGE: campaigns_api сам extract'ит content в python, если mode=user.

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

  function getUserEditorHtml() {
    try {
      const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
      return ed ? (ed.getContent({ format: "html" }) || "") : "";
    } catch (_) {
      return "";
    }
  }

  function getAdvHtml() {
    try {
      if (typeof window.yyCampEnsureCodeMirror === "function") window.yyCampEnsureCodeMirror();
      return typeof window.yyCampAdvGetHtml === "function" ? (window.yyCampAdvGetHtml() || "") : "";
    } catch (_) {
      return "";
    }
  }

  function previewCurrent() {
    if (!window.YYModal || typeof window.YYModal.open !== "function") return;

    const id = getCampaignId();
    if (!id) return;

    const mode = getMode();
    const html = mode === "advanced" ? normalizeTabsTo2Spaces(getAdvHtml()) : (getUserEditorHtml() || "");

    window.YYModal.open("post=/panel/campaigns/campaigns/preview/modal-from-editor/", {
      id: id,
      editor_mode: mode,
      editor_html: html || "",
    });
  }

  window.yyCampPreviewCurrent = previewCurrent;
})();
