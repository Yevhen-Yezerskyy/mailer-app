// FILE: web/static/js/campaign_templates/codemirror.js
// DATE: 2026-01-17
// PURPOSE: CodeMirror runtime for advanced HTML/CSS editors (NO textarea fallback).
// CHANGE: добавлен yyTplAdvRefresh() чтобы CM не был "пустым" после показа hidden-блока.

(function () {
  "use strict";

  const $ = (s) => document.querySelector(s);

  let cmHtml = null;
  let cmCss = null;

  function resolveFlowEditorHeight() {
    const raw = Number(window.yyTplFlowEditorHeight || 0);
    if (Number.isFinite(raw) && raw > 240) return Math.floor(raw);
    return 700;
  }

  function ensure() {
    if (cmHtml && cmCss) return true;
    if (!window.CodeMirror) return false;

    const advHtml = $("#yyAdvHtml");
    const advCss = $("#yyAdvCss");
    if (!advHtml || !advCss) return false;

    const common = {
      theme: "material-darker",
      lineNumbers: true,
      lineWrapping: true,
      indentUnit: 2,
      tabSize: 2,
      indentWithTabs: false,
      smartIndent: true,
      extraKeys: {
        Tab: (cm) => cm.execCommand("insertSoftTab"),
        "Shift-Tab": (cm) => cm.execCommand("indentLess"),
      },
    };

    cmHtml = window.CodeMirror.fromTextArea(advHtml, Object.assign({}, common, { mode: "htmlmixed" }));
    cmCss = window.CodeMirror.fromTextArea(advCss, Object.assign({}, common, { mode: "css" }));

    const h = resolveFlowEditorHeight();
    const px = String(h) + "px";
    cmHtml.setSize("100%", px);
    cmCss.setSize("100%", px);

    return true;
  }

  window.yyTplEnsureCodeMirror = function () {
    return ensure();
  };

  window.yyTplAdvRefresh = function () {
    if (!ensure()) return;
    try { cmHtml && cmHtml.refresh(); } catch (_) {}
    try { cmCss && cmCss.refresh(); } catch (_) {}
  };

  window.yyTplAdvGetHtml = function () {
    if (!ensure() || !cmHtml) return "";
    return cmHtml.getValue() || "";
  };

  window.yyTplAdvGetCss = function () {
    if (!ensure() || !cmCss) return "";
    return cmCss.getValue() || "";
  };

  window.yyTplAdvSet = function (html, css) {
    if (!ensure()) return false;
    try {
      cmHtml.setValue(html == null ? "" : String(html));
      cmCss.setValue(css == null ? "" : String(css));
      return true;
    } catch (_) {
      return false;
    }
  };

  window.yyTplSetCodeMirrorHeight = function (h) {
    if (!ensure() || !cmHtml || !cmCss) return;
    const raw = Number(h || 0);
    const height = Number.isFinite(raw) && raw > 240 ? Math.floor(raw) : resolveFlowEditorHeight();
    const px = String(height) + "px";
    try {
      cmHtml.setSize("100%", px);
      cmCss.setSize("100%", px);
      cmHtml.refresh();
      cmCss.refresh();
    } catch (_) {}
  };
})();
