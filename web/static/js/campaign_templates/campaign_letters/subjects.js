// FILE: web/static/js/campaign_letters/subjects.js
// DATE: 2026-01-19
// PURPOSE: Subjects UI: 1..3 инпута + кнопка "+"; initial from JSON.
// CHANGE: (new) сбор и валидация в submit.js.

(function () {
  "use strict";

  const $ = (s) => document.querySelector(s);

  function safeParseJson(s) {
    try { return JSON.parse(s || "[]"); } catch (_) { return []; }
  }

  function mkInput(val) {
    const inp = document.createElement("input");
    inp.type = "text";
    inp.className = "YY-INPUT !mb-0";
    inp.placeholder = "Subject";
    inp.value = val || "";
    inp.dataset.yySubject = "1";
    return inp;
  }

  function init() {
    const wrap = $("#yySubjectsWrap");
    const addBtn = $("#yySubjectsAddBtn");
    const src = $("#yyInitSubjects");
    if (!wrap || !addBtn || !src) return;

    const arr = safeParseJson(src.value || "[]");
    const vals = Array.isArray(arr) ? arr.slice(0, 3).map((x) => String(x || "").trim()) : [];
    const nonEmpty = vals.filter((x) => x);

    const start = nonEmpty.length ? nonEmpty : [""];
    start.forEach((v) => wrap.appendChild(mkInput(v)));

    addBtn.addEventListener("click", function () {
      const current = wrap.querySelectorAll('input[data-yy-subject="1"]').length;
      if (current >= 3) return;
      wrap.appendChild(mkInput(""));
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
