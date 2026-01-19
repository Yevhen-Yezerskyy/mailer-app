// FILE: web/static/js/campaign_letters/codemirror.js
// DATE: 2026-01-19
// PURPOSE: CodeMirror runtime для advanced HTML (один редактор).
// CHANGE: (new) yyCampEnsureCodeMirror/yyCampAdvGetHtml/yyCampAdvSet/yyCampAdvRefresh.

(function () {
  "use strict";

  const $ = (s) => document.querySelector(s);

  let cmHtml = null;

  function ensure() {
    if (cmHtml) return true;
    if (!window.CodeMirror) return false;

    const advHtml = $("#yyAdvHtml");
    if (!advHtml) return false;

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
      mode: "htmlmixed",
    };

    cmHtml = window.CodeMirror.fromTextArea(advHtml, common);
    cmHtml.setSize("100%", "700px");
    return true;
  }

  window.yyCampEnsureCodeMirror = function () {
    return ensure();
  };

  window.yyCampAdvRefresh = function () {
    if (!ensure()) return;
    try { cmHtml && cmHtml.refresh(); } catch (_) {}
  };

  window.yyCampAdvGetHtml = function () {
    if (!ensure() || !cmHtml) return "";
    return cmHtml.getValue() || "";
  };

  window.yyCampAdvSet = function (html) {
    if (!ensure() || !cmHtml) return false;
    try {
      cmHtml.setValue(html == null ? "" : String(html));
      return true;
    } catch (_) {
      return false;
    }
  };
})();
