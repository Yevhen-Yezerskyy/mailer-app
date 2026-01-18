// FILE: web/static/js/campaign_templates/tinymce.js
// DATE: 2026-01-18
// PURPOSE: TinyMCE runtime: базовый CSS + overlays (colors/fonts) как отдельные <style> в iframe.
// CHANGE:
//   - ВОССТАНОВЛЕНО: yyTplApplyOverlay() + раздельные style-теги (base/color/font), чтобы colors/fonts не ломались.
//   - yyTplSetCss(css) ставит БАЗУ и сбрасывает overlays.
//   - yyTplGetCss() возвращает итоговый CSS (base + overlays) для сохранения.
//   - Загрузка: если в URL есть gl_tpl — грузим GlobalTemplate (HTML+CSS) независимо от state; иначе edit по id.

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

  const IDS = {
    base: "yyCssBase",
    color: "yyCssOverlayColor",
    font: "yyCssOverlayFont",
  };

  function ensureStyle(editor, id) {
    const doc = editor && editor.getDoc ? editor.getDoc() : null;
    if (!doc) return null;

    let el = doc.getElementById(id);
    if (el) return el;

    el = doc.createElement("style");
    el.id = id;
    el.type = "text/css";
    (doc.head || doc.documentElement).appendChild(el);
    return el;
  }

  function setBaseCss(editor, cssText) {
    const base = ensureStyle(editor, IDS.base);
    if (base) base.textContent = String(cssText || "").trim();
  }

  function setOverlayCss(editor, type, cssText) {
    const id = type === "colors" ? IDS.color : IDS.font;
    const el = ensureStyle(editor, id);
    if (el) el.textContent = String(cssText || "").trim();
  }

  function clearOverlays(editor) {
    setOverlayCss(editor, "colors", "");
    setOverlayCss(editor, "fonts", "");
  }

  function getAllCss(editor) {
    const doc = editor && editor.getDoc ? editor.getDoc() : null;
    if (!doc) return "";

    const base = (doc.getElementById(IDS.base)?.textContent || "").trim();
    const c = (doc.getElementById(IDS.color)?.textContent || "").trim();
    const f = (doc.getElementById(IDS.font)?.textContent || "").trim();

    let out = "";
    if (base) out += base + "\n";
    if (c) out += "\n" + c + "\n";
    if (f) out += "\n" + f + "\n";
    return out.trim() + (out.trim() ? "\n" : "");
  }

  // --- globals ---
  window.yyTplSetCss = function (css) {
    try {
      const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
      if (!ed) return;
      setBaseCss(ed, css || "");
      clearOverlays(ed);
    } catch (_) {}
  };

  window.yyTplApplyOverlay = function (type, css) {
    try {
      const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
      if (!ed) return;
      if (type !== "colors" && type !== "fonts") return;
      setOverlayCss(ed, type, css || "");
    } catch (_) {}
  };

  window.yyTplGetCss = function () {
    try {
      const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
      if (!ed) return "";
      return getAllCss(ed);
    } catch (_) {
      return "";
    }
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
    const glTpl = (getParam("gl_tpl") || "").trim();

    // 1) OVERRIDE: если есть gl_tpl — грузим GlobalTemplate независимо от state.
    if (glTpl) {
      const g = encodeURIComponent(glTpl);
      const urlHtml = `/panel/campaigns/templates/_render-user-html/?gl_tpl=${g}`;
      const urlCss = `/panel/campaigns/templates/_render-user-css/?gl_tpl=${g}`;

      fetchText(urlHtml)
        .then((html) => {
          try {
            editor.setContent(html || "");
          } catch (_) {}
          return fetchText(urlCss);
        })
        .then((css) => window.yyTplSetCss(css || ""))
        .catch(() => {});
      return;
    }

    // 2) edit: грузим текущий Templates по id.
    if (state === "edit" && uiId) {
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
      return;
    }

    // default
    try {
      editor.setContent("");
    } catch (_) {}
    window.yyTplSetCss("");
  }

  window.yyTplRuntimeOnEditorInit = function (editor) {
    ensureStyle(editor, IDS.base);
    ensureStyle(editor, IDS.color);
    ensureStyle(editor, IDS.font);
    loadExistingInto(editor);
  };

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
