// FILE: web/static/js/campaign_templates/codemirror.js
// DATE: 2026-01-17
// PURPOSE: CodeMirror runtime for advanced HTML/CSS editors (NO textarea fallback).
// CHANGE: добавлен yyTplAdvRefresh() чтобы CM не был "пустым" после показа hidden-блока.

(function () {
  "use strict";

  const $ = (s) => document.querySelector(s);

  let cmHtml = null;
  let cmCss = null;

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

    cmHtml.setSize("100%", "700px");
    cmCss.setSize("100%", "700px");

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
})();
