// FILE: web/static/js/campaign_templates_editor.js
// DATE: 2026-01-14
// PURPOSE: User-mode only (Quill).
// CHANGE: Quill получает только HTML; CSS живёт отдельно (1 live <style>), запоминаем lastCssText.
//         Save отправляет editor_html + css_text.

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
    const host = $("#yyQuillStyleHost");
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

    const editorEl = $("#yyQuillEditor");
    const hiddenHtml = $("#yyEditorHtml");
    const hiddenCss = $("#yyCssText");
    if (!editorEl || !hiddenHtml || !hiddenCss) return;

    if (!window.Quill) {
      console.error("Quill not loaded");
      return;
    }

    const quill = new window.Quill(editorEl, {
      theme: "snow",
      modules: { toolbar: true },
    });

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

    if (state === "edit" && uiId) {
      const id = encodeURIComponent(uiId);
      const urlHtml = `/panel/campaigns/templates/render-user-html/?id=${id}`;
      const urlCss = `/panel/campaigns/templates/render-user-css/?id=${id}`;

      fetchText(urlHtml)
        .then((html) => {
          quill.root.innerHTML = html || "";
          return fetchText(urlCss);
        })
        .then((css) => {
          applyCss(css);
        })
        .catch((e) => console.error(e));
    } else {
      applyCss("");
    }

    form.addEventListener("submit", () => {
      hiddenHtml.value = quill.root.innerHTML || "";
      hiddenCss.value = lastCssText || "";
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
