// FILE: web/static/js/campaign_templates/campaign_letters/submit.js
// DATE: 2026-01-20
// PURPOSE: Submit: никаких разборов HTML на клиенте. В user-mode шлем visual editor_html, python достанет content.
// CHANGE: editor_mode + editor_html (visual или content) кладем в hidden.

(function () {
  "use strict";

  const $ = (s) => document.querySelector(s);

  function normalizeTabsTo2Spaces(s) {
    return (s || "").replace(/\t/g, "  ");
  }

  function getMode() {
    const el = $("#yyEditorMode");
    const v = el ? String(el.value || "").trim() : "user";
    return v === "advanced" ? "advanced" : "user";
  }

  function getUserEditorHtml() {
    try {
      const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
      return ed ? (ed.getContent({ format: "html" }) || "") : "";
    } catch (_) {
      return "";
    }
  }

  function getAdvHtml() {
    try {
      if (typeof window.yyCampEnsureCodeMirror === "function") window.yyCampEnsureCodeMirror();
      return typeof window.yyCampAdvGetHtml === "function" ? (window.yyCampAdvGetHtml() || "") : "";
    } catch (_) {
      return "";
    }
  }

  function collectSubjects(wrapId) {
    const wrap = $(wrapId);
    if (!wrap) return [];
    const inputs = wrap.querySelectorAll('input[data-yy-subject="1"]');
    const out = [];
    inputs.forEach((inp) => {
      const v = String(inp.value || "").trim();
      if (v) out.push(v);
    });
    return out.slice(0, 3);
  }

  function init() {
    const form = $("#yySendingForm");
    if (!form) return;

    const hiddenHtml = $("#yyEditorHtml");
    const hiddenSubs = $("#yySubjectsJson");
    if (!hiddenHtml || !hiddenSubs) return;

    form.addEventListener("submit", function (e) {
      const btn = e.submitter || document.activeElement;
      const action = btn && btn.value ? String(btn.value).trim() : "";
      if (action !== "save_letter" && action !== "save_ready") return;

      const mode = getMode();
      hiddenHtml.value = mode === "advanced" ? normalizeTabsTo2Spaces(getAdvHtml()) : (getUserEditorHtml() || "");

      // subjects берем из user-wrap (он у тебя один и тот же набор инпутов)
      hiddenSubs.value = JSON.stringify(collectSubjects("#yySubjectsWrap"));
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
