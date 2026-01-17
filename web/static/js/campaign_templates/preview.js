// FILE: web/static/js/campaign_templates/preview.js
// DATE: 2026-01-17
// PURPOSE: Preview через YYModal (GET by id, POST from editor).
// CHANGE: user-mode теперь берёт css через yyTplGetCss; advanced-mode гарантированно инициализирует CodeMirror.

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

  function getUserEditorHtml() {
    try {
      const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
      return ed ? (ed.getContent({ format: "html" }) || "") : "";
    } catch (_) {
      return "";
    }
  }

  function getUserCss() {
    try {
      if (typeof window.yyTplGetCss === "function") return window.yyTplGetCss() || "";
    } catch (_) {}
    return "";
  }

  function previewById(uiId) {
    const id = String(uiId || "").trim();
    if (!id) return;
    if (!window.YYModal || typeof window.YYModal.open !== "function") return;

    window.YYModal.open(`url=/panel/campaigns/templates/preview/modal/?id=${encodeURIComponent(id)}`);
  }

  function previewCurrent() {
    if (!window.YYModal || typeof window.YYModal.open !== "function") return;

    const mode = getMode();

    if (mode === "advanced") {
      if (typeof window.yyTplEnsureCodeMirror === "function") window.yyTplEnsureCodeMirror();

      const tpl = normalizeTabsTo2Spaces(
        typeof window.yyTplAdvGetHtml === "function" ? window.yyTplAdvGetHtml() : ""
      );
      const css = normalizeTabsTo2Spaces(
        typeof window.yyTplAdvGetCss === "function" ? window.yyTplAdvGetCss() : ""
      );

      if (!tpl && !css) return;

      window.YYModal.open("post=/panel/campaigns/templates/preview/modal-from-editor/", {
        mode: "advanced",
        template_html: tpl,
        css_text: css,
      });
      return;
    }

    const html = getUserEditorHtml();
    const css = getUserCss();

    window.YYModal.open("post=/panel/campaigns/templates/preview/modal-from-editor/", {
      mode: "user",
      editor_html: html || "",
      css_text: css || "",
    });
  }

  window.yyTplPreviewById = previewById;
  window.yyTplPreviewCurrent = previewCurrent;
})();
