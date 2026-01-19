// FILE: web/static/js/campaign_letters/tinymce.js
// DATE: 2026-01-19
// PURPOSE: TinyMCE runtime: инициализация + загрузка стартового HTML из скрытого textarea.
// CHANGE: (new) без CSS/overlays.

(function () {
  "use strict";

  const $ = (s) => document.querySelector(s);

  function initContent(editor) {
    const src = $("#yyInitHtml");
    const html = src ? (src.value || "") : "";
    try {
      editor.setContent(html || "");
    } catch (_) {}
  }

  window.yyCampRuntimeOnEditorInit = function (editor) {
    initContent(editor);
  };

  window.YYCampaignLetterTiny = {
    getHtml: function () {
      try {
        const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
        return ed ? (ed.getContent({ format: "html" }) || "") : "";
      } catch (_) {
        return "";
      }
    },
    setHtml: function (html) {
      try {
        const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
        if (ed) ed.setContent(html || "");
      } catch (_) {}
    },
  };

  function init() {
    const ta = $("#yyTinyEditor");
    if (!ta) return;
    if (!window.tinymce || typeof window.yyCampTinyBuildConfig !== "function") return;
    window.tinymce.init(window.yyCampTinyBuildConfig());
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
