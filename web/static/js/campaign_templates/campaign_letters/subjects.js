// FILE: web/static/js/campaign_templates/campaign_letters/subjects.js
// DATE: 2026-01-20
// PURPOSE: Campaign Letter Subjects: 3 фиксированных инпута (ротация), initial из yyInitSubjects.

(function () {
  "use strict";

  const $ = (s) => document.querySelector(s);

  function safeParseJson(s) {
    try {
      const v = JSON.parse(s || "[]");
      return Array.isArray(v) ? v : [];
    } catch (_) {
      return [];
    }
  }

  function setVal(id, v) {
    const el = $(id);
    if (!el) return;
    el.value = v || "";
  }

  function init() {
    const s1 = $("#yySubject1");
    const s2 = $("#yySubject2");
    const s3 = $("#yySubject3");
    const src = $("#yyInitSubjects");
    if (!s1 || !s2 || !s3 || !src) return;

    const arr = safeParseJson(src.value || "[]")
      .map((x) => String(x == null ? "" : x).trim())
      .filter((x) => x);

    setVal("#yySubject1", arr[0] || "");
    setVal("#yySubject2", arr[1] || "");
    setVal("#yySubject3", arr[2] || "");
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
