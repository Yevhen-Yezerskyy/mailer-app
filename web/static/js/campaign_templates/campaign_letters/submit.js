// FILE: web/static/js/campaign_templates/campaign_letters/submit.js
// DATE: 2026-01-20
// PURPOSE: Submit: собрать editor_html + subjects(3 обязательных) + headers_json.
// CHANGE: запрет сабмита если любой subject пустой; красная обводка пустых полей.

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

  function setErr(el, isErr) {
    if (!el) return;
    if (isErr) {
      el.dataset.yyErr = "1";
      el.style.borderColor = "#ef4444";
      el.style.boxShadow = "0 0 0 2px rgba(239, 68, 68, 0.20)";
    } else {
      delete el.dataset.yyErr;
      el.style.borderColor = "";
      el.style.boxShadow = "";
    }
  }

  function bindClearOnInput(el) {
    if (!el) return;
    el.addEventListener("input", function () {
      const v = String(el.value || "").trim();
      setErr(el, !v);
    });
    el.addEventListener("blur", function () {
      const v = String(el.value || "").trim();
      setErr(el, !v);
    });
  }

  function validateSubjectsRequired() {
    const s1 = $("#yySubject1");
    const s2 = $("#yySubject2");
    const s3 = $("#yySubject3");

    const v1 = s1 ? String(s1.value || "").trim() : "";
    const v2 = s2 ? String(s2.value || "").trim() : "";
    const v3 = s3 ? String(s3.value || "").trim() : "";

    setErr(s1, !v1);
    setErr(s2, !v2);
    setErr(s3, !v3);

    if (!v1 || !v2 || !v3) {
      const firstBad = (!v1 && s1) || (!v2 && s2) || (!v3 && s3) || null;
      try { firstBad && firstBad.focus(); } catch (_) {}
      return null;
    }

    return [v1, v2, v3];
  }

  function getHeadersJsonText(mode) {
    // В visual-mode поле не видно => не затираем, берём init/hidden.
    if (mode !== "advanced") {
      const src = $("#yyInitHeaders");
      return String((src && src.value) || "{}");
    }

    try {
      if (typeof window.yyCampEnsureCodeMirror === "function") window.yyCampEnsureCodeMirror();
      if (typeof window.yyCampHeadersGet === "function") return String(window.yyCampHeadersGet() || "{}");
    } catch (_) {}

    const ta = $("#yyHeadersJsonArea");
    return String((ta && ta.value) || "{}");
  }

  function init() {
    const form = $("#yySendingForm");
    if (!form) return;

    const hiddenHtml = $("#yyEditorHtml");
    const hiddenSubs = $("#yySubjectsJson");
    const hiddenHeaders = $("#yyHeadersJson");
    if (!hiddenHtml || !hiddenSubs || !hiddenHeaders) return;

    // default headers hidden сразу
    const initHeaders = $("#yyInitHeaders");
    hiddenHeaders.value = String((initHeaders && initHeaders.value) || "{}");

    // bind live clear
    bindClearOnInput($("#yySubject1"));
    bindClearOnInput($("#yySubject2"));
    bindClearOnInput($("#yySubject3"));

    form.addEventListener("submit", function (e) {
      const btn = e.submitter || document.activeElement;
      const action = btn && btn.value ? String(btn.value).trim() : "";
      if (action !== "save_letter" && action !== "save_ready") return;

      const subs = validateSubjectsRequired();
      if (!subs) {
        e.preventDefault();
        return;
      }

      const mode = getMode();
      hiddenHtml.value = mode === "advanced" ? normalizeTabsTo2Spaces(getAdvHtml()) : (getUserEditorHtml() || "");
      hiddenSubs.value = JSON.stringify(subs);
      hiddenHeaders.value = getHeadersJsonText(mode);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
