// FILE: web/static/js/campaign_letters/mode_switch.js
// DATE: 2026-01-19
// PURPOSE: Переключение user<->advanced (TinyMCE <-> CodeMirror) без сервера.
// CHANGE: (new) кнопки всегда работают даже при пустом HTML.

(function () {
  "use strict";

  const $ = (s) => document.querySelector(s);

  function normalizeTabsTo2Spaces(s) {
    return (s || "").replace(/\t/g, "  ");
  }

  function setMode(mode) {
    const el = $("#yyEditorMode");
    if (el) el.value = mode === "advanced" ? "advanced" : "user";
  }

  function showUserMode() {
    const aL = $("#yyAdvancedModeLeft");
    const aR = $("#yyAdvancedModeRight");
    const uL = $("#yyUserModeLeft");
    const uR = $("#yyUserModeRight");

    if (aL) aL.classList.add("hidden");
    if (aR) aR.classList.add("hidden");
    if (uL) uL.classList.remove("hidden");
    if (uR) uR.classList.remove("hidden");

    setMode("user");
  }

  function showAdvancedMode() {
    const aL = $("#yyAdvancedModeLeft");
    const aR = $("#yyAdvancedModeRight");
    const uL = $("#yyUserModeLeft");
    const uR = $("#yyUserModeRight");

    if (uL) uL.classList.add("hidden");
    if (uR) uR.classList.add("hidden");
    if (aL) aL.classList.remove("hidden");
    if (aR) aR.classList.remove("hidden");

    setMode("advanced");
  }

  async function switchToAdvanced() {
    try {
      const html = normalizeTabsTo2Spaces(
        (window.YYCampaignLetterTiny && window.YYCampaignLetterTiny.getHtml)
          ? window.YYCampaignLetterTiny.getHtml()
          : ""
      );

      showAdvancedMode();

      if (typeof window.yyCampEnsureCodeMirror === "function") window.yyCampEnsureCodeMirror();
      if (typeof window.yyCampAdvSet === "function") window.yyCampAdvSet(html || "");

      if (typeof window.yyCampAdvRefresh === "function") setTimeout(() => window.yyCampAdvRefresh(), 0);
    } catch (_) {}
  }

  async function switchToUser() {
    try {
      if (typeof window.yyCampEnsureCodeMirror === "function") window.yyCampEnsureCodeMirror();

      const html = normalizeTabsTo2Spaces(
        typeof window.yyCampAdvGetHtml === "function" ? window.yyCampAdvGetHtml() : ""
      );

      if (window.YYCampaignLetterTiny && window.YYCampaignLetterTiny.setHtml) {
        window.YYCampaignLetterTiny.setHtml(html || "");
      }

      showUserMode();
    } catch (_) {}
  }

  window.yyCampSwitchToAdvanced = switchToAdvanced;
  window.yyCampSwitchToUser = switchToUser;

  function init() {
    showUserMode();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
