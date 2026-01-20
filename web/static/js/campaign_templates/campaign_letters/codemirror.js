// FILE: web/static/js/campaign_templates/campaign_letters/codemirror.js
// DATE: 2026-01-20
// PURPOSE: CodeMirror runtime для advanced HTML + JSON headers.
// CHANGE: если headers пустые — инициализировать "{}" ТОЛЬКО для правого JSON-редактора.

(function () {
  "use strict";

  const $ = (s) => document.querySelector(s);

  let cmHtml = null;
  let cmHeaders = null;

  function commonBase() {
    return {
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
  }

  function ensureHtml() {
    if (cmHtml) return true;
    if (!window.CodeMirror) return false;

    const advHtml = $("#yyAdvHtml");
    if (!advHtml) return false;

    const cfg = Object.assign(commonBase(), { mode: "htmlmixed" });
    cmHtml = window.CodeMirror.fromTextArea(advHtml, cfg);
    cmHtml.setSize("100%", "700px");
    return true;
  }

  function ensureHeaders() {
    if (cmHeaders) return true;
    if (!window.CodeMirror) return false;

    const ta = $("#yyHeadersJsonArea");
    if (!ta) return false;

    // initial value из hidden yyInitHeaders
    const src = $("#yyInitHeaders");
    let v = src ? String(src.value || "").trim() : "";

    // ВАЖНО: если вообще пусто — ставим "{}"
    if (!v) v = "{}";
    ta.value = v;

    const cfg = Object.assign(commonBase(), {
      mode: { name: "javascript", json: true },
    });
    cmHeaders = window.CodeMirror.fromTextArea(ta, cfg);
    cmHeaders.setSize("100%", "240px");
    return true;
  }

  function ensure() {
    const okHtml = ensureHtml();
    ensureHeaders();
    return okHtml;
  }

  window.yyCampEnsureCodeMirror = function () {
    return ensure();
  };

  window.yyCampAdvRefresh = function () {
    ensure();
    try { cmHtml && cmHtml.refresh(); } catch (_) {}
    try { cmHeaders && cmHeaders.refresh(); } catch (_) {}
  };

  window.yyCampAdvGetHtml = function () {
    if (!ensureHtml() || !cmHtml) return "";
    return cmHtml.getValue() || "";
  };

  window.yyCampAdvSet = function (html) {
    if (!ensureHtml() || !cmHtml) return false;
    try {
      cmHtml.setValue(html == null ? "" : String(html));
      return true;
    } catch (_) {
      return false;
    }
  };

  window.yyCampHeadersGet = function () {
    try {
      ensureHeaders();
      return cmHeaders ? (cmHeaders.getValue() || "") : (($("#yyHeadersJsonArea") || {}).value || "");
    } catch (_) {
      return "";
    }
  };
})();
