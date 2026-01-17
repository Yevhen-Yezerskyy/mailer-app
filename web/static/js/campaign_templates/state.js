// FILE: web/static/js/campaign_templates/state.js  (новое — 2026-01-17)
// PURPOSE: Общее состояние для редактора шаблонов (mode, css, доступ к CM/Tiny, preview-request).

(function () {
  "use strict";

  const S = {
    mode: "user",
    lastCssText: "",
    cm: { html: null, css: null },
    previewReq: null, // {kind:'by_id', id} | {kind:'from_editor', body}
  };

  function setMode(m) {
    S.mode = m === "advanced" ? "advanced" : "user";
    const el = document.getElementById("yyEditorMode");
    if (el) el.value = S.mode;
  }

  function getMode() {
    const el = document.getElementById("yyEditorMode");
    const v = el ? (el.value || "") : "";
    return v === "advanced" ? "advanced" : "user";
  }

  function setCss(cssText) {
    S.lastCssText = (cssText || "").trim();
  }

  function getCss() {
    return S.lastCssText || "";
  }

  function setCodeMirror(htmlCm, cssCm) {
    S.cm.html = htmlCm || null;
    S.cm.css = cssCm || null;
  }

  function getCmHtml() {
    return S.cm.html;
  }

  function getCmCss() {
    return S.cm.css;
  }

  function setPreviewReq(req) {
    S.previewReq = req || null;
  }

  function getPreviewReq() {
    return S.previewReq;
  }

  window.YYCampaignTplState = {
    setMode,
    getMode,
    setCss,
    getCss,
    setCodeMirror,
    getCmHtml,
    getCmCss,
    setPreviewReq,
    getPreviewReq,
  };
})();
