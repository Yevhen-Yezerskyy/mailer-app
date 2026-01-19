// FILE: web/static/js/campaign_letters/submit.js
// DATE: 2026-01-19
// PURPOSE: Перед submit заполняет hidden editor_html + subjects_json.
// CHANGE: (new) работает для save_letter/save_ready.

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

  function getUserHtml() {
    try {
      return (window.YYCampaignLetterTiny && window.YYCampaignLetterTiny.getHtml)
        ? (window.YYCampaignLetterTiny.getHtml() || "")
        : "";
    } catch (_) {
      return "";
    }
  }

  function getAdvHtml() {
    try {
      if (typeof window.yyCampEnsureCodeMirror === "function") window.yyCampEnsureCodeMirror();
      return (typeof window.yyCampAdvGetHtml === "function") ? (window.yyCampAdvGetHtml() || "") : "";
    } catch (_) {
      return "";
    }
  }

  function collectSubjects() {
    const wrap = $("#yySubjectsWrap");
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

      // only for letter save actions
      if (action !== "save_letter" && action !== "save_ready") return;

      const mode = getMode();
      hiddenHtml.value = (mode === "advanced") ? normalizeTabsTo2Spaces(getAdvHtml()) : (getUserHtml() || "");
      hiddenSubs.value = JSON.stringify(collectSubjects());
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
