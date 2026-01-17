// FILE: web/static/js/campaign_templates/tinymce.js
// DATE: 2026-01-17
// PURPOSE: TinyMCE runtime: live-css storage + apply to editor iframe + загрузка existing html/css в edit.
// CHANGE: добавлены глобальные yyTplSetCss/yyTplGetCss, чтобы preview мог брать css в user-mode.

(function () {
  "use strict";

  const $ = (s) => document.querySelector(s);

  function getParam(name) {
    try {
      return new URLSearchParams(window.location.search).get(name);
    } catch {
      return null;
    }
  }

  async function fetchText(url) {
    const r = await fetch(url, { credentials: "same-origin" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.text();
  }

  // --- CSS state (global) ---
  let lastCssText = "";

  function ensureIframeStyle(editor) {
    const doc = editor && editor.getDoc ? editor.getDoc() : null;
    if (!doc) return null;

    let el = doc.getElementById("yyLiveCss");
    if (el) return el;

    el = doc.createElement("style");
    el.id = "yyLiveCss";
    el.type = "text/css";
    (doc.head || doc.documentElement).appendChild(el);
    return el;
  }

  function applyCssToEditor(editor, cssText) {
    lastCssText = (cssText || "").trim();
    const styleEl = ensureIframeStyle(editor);
    if (styleEl) styleEl.textContent = lastCssText;
  }

  // globals for preview + other modules
  window.yyTplSetCss = function (css) {
    lastCssText = (css || "").trim();
    try {
      const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
      if (ed) applyCssToEditor(ed, lastCssText);
    } catch (_) {}
  };

  window.yyTplGetCss = function () {
    return lastCssText || "";
  };

  window.YYCampaignTplTiny = {
    getEditorHtml: function () {
      try {
        const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
        return ed ? (ed.getContent({ format: "html" }) || "") : "";
      } catch (_) {
        return "";
      }
    },
    setEditorHtml: function (html) {
      try {
        const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
        if (ed) ed.setContent(html || "");
      } catch (_) {}
    },
  };

  function loadExistingInto(editor) {
    const state = (getParam("state") || "").trim();
    const uiId = (getParam("id") || "").trim();

    if (!(state === "edit" && uiId)) {
      try {
        editor.setContent("");
      } catch (_) {}
      window.yyTplSetCss("");
      return;
    }

    const id = encodeURIComponent(uiId);
    const urlHtml = `/panel/campaigns/templates/_render-user-html/?id=${id}`;
    const urlCss = `/panel/campaigns/templates/_render-user-css/?id=${id}`;

    fetchText(urlHtml)
      .then((html) => {
        try {
          editor.setContent(html || "");
        } catch (_) {}
        return fetchText(urlCss);
      })
      .then((css) => window.yyTplSetCss(css || ""))
      .catch(() => {});
  }

  // called from tinymce_config.js on init
  window.yyTplRuntimeOnEditorInit = function (editor) {
    applyCssToEditor(editor, lastCssText);
    loadExistingInto(editor);
  };

  // init Tiny if config available
  function init() {
    const ta = $("#yyTinyEditor");
    if (!ta) return;
    if (!window.tinymce || typeof window.yyTinyBuildConfig !== "function") return;
    window.tinymce.init(window.yyTinyBuildConfig());
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
