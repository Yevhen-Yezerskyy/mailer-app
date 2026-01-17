// FILE: web/static/js/campaign_templates/submit.js
// DATE: 2026-01-17
// PURPOSE: Перед submit заполняет hidden поля editor_html/css_text (единый пайплайн сохранения на сервере).
// CHANGE: advanced берём ТОЛЬКО из CodeMirror-глобалок (yyTplAdvGetHtml/yyTplAdvGetCss), без textarea.
//         user берём из Tiny + yyTplGetCss.

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

  function getUserHtml() {
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

  function getAdvHtml() {
    try {
      if (typeof window.yyTplEnsureCodeMirror === "function") window.yyTplEnsureCodeMirror();
      if (typeof window.yyTplAdvGetHtml === "function") return window.yyTplAdvGetHtml() || "";
    } catch (_) {}
    return "";
  }

  function getAdvCss() {
    try {
      if (typeof window.yyTplEnsureCodeMirror === "function") window.yyTplEnsureCodeMirror();
      if (typeof window.yyTplAdvGetCss === "function") return window.yyTplAdvGetCss() || "";
    } catch (_) {}
    return "";
  }

  function init() {
    const form = $("#yyTplForm");
    if (!form) return;

    const hiddenHtml = $("#yyEditorHtml");
    const hiddenCss = $("#yyCssText");
    if (!hiddenHtml || !hiddenCss) return;

    form.addEventListener("submit", () => {
      const mode = getMode();

      if (mode === "advanced") {
        hiddenHtml.value = normalizeTabsTo2Spaces(getAdvHtml());
        hiddenCss.value = normalizeTabsTo2Spaces(getAdvCss());
        return;
      }

      hiddenHtml.value = getUserHtml() || "";
      hiddenCss.value = getUserCss() || "";
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
