// FILE: web/static/js/campaign_templates_editor.js
// DATE: 2026-01-14
// PURPOSE: User-mode runtime (TinyMCE inline).
// CHANGE: Runtime отдельно от init-config: live <style>, загрузка HTML/CSS, submit -> hidden поля.
//         Конфиг берём из window.yyTinyBuildConfig().

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

  function ensureLiveStyleEl() {
    const host = $("#yyTinyStyleHost");
    let el = document.getElementById("yyLiveCss");
    if (el) return el;

    el = document.createElement("style");
    el.id = "yyLiveCss";
    el.type = "text/css";

    if (host) {
      host.innerHTML = "";
      host.appendChild(el);
    } else {
      (document.head || document.documentElement).appendChild(el);
    }
    return el;
  }

  function init() {
    const form = $("#yyTplForm");
    if (!form) return;

    const editorEl = $("#yyTinyEditor");
    const hiddenHtml = $("#yyEditorHtml");
    const hiddenCss = $("#yyCssText");
    if (!editorEl || !hiddenHtml || !hiddenCss) return;

    if (!window.tinymce || typeof window.yyTinyBuildConfig !== "function") {
      console.error("TinyMCE or config not loaded");
      return;
    }

    const liveStyle = ensureLiveStyleEl();
    let lastCssText = "";

    function applyCss(cssText) {
      lastCssText = (cssText || "").trim();
      liveStyle.textContent = lastCssText;
    }

    // публично (на будущее для коллектора)
    window.yyTplSetCss = function (cssText) {
      applyCss(cssText);
    };
    window.yyTplGetCss = function () {
      return lastCssText;
    };

    const state = (getParam("state") || "").trim();
    const uiId = (getParam("id") || "").trim();

    function loadExistingInto(editor) {
      if (!(state === "edit" && uiId)) {
        editor.setContent("");
        applyCss("");
        return;
      }

      const id = encodeURIComponent(uiId);
      const urlHtml = `/panel/campaigns/templates/render-user-html/?id=${id}`;
      const urlCss = `/panel/campaigns/templates/render-user-css/?id=${id}`;

      fetchText(urlHtml)
        .then((html) => {
          editor.setContent(html || "");
          return fetchText(urlCss);
        })
        .then((css) => applyCss(css))
        .catch((e) => console.error(e));
    }

    // Хук для init-config
    window.yyTplRuntimeOnEditorInit = function (editor) {
      loadExistingInto(editor);
    };

    // init TinyMCE (конфиг отдельно)
    window.tinymce.init(window.yyTinyBuildConfig(editorEl));

    form.addEventListener("submit", () => {
      const ed = window.tinymce.get(editorEl.id);
      const html = ed ? ed.getContent({ format: "html" }) : (editorEl.innerHTML || "");
      hiddenHtml.value = html || "";
      hiddenCss.value = lastCssText || "";
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
