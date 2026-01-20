// FILE: web/static/js/campaign_templates/campaign_letters/mode_switch.js
// DATE: 2026-01-20
// PURPOSE: Переключение user<->advanced ТОЛЬКО через python (как в templates).
// CHANGE: switchToAdvanced: POST _extract-content (editor_html visual -> content_html).
//         switchToUser:     POST _render-editor-html (content_html -> editor_html visual).

(function () {
  "use strict";

  const $ = (s) => document.querySelector(s);

  async function postJson(url, payload) {
    const r = await fetch(url, {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload || {}),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.json();
  }

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

  function getCampaignId() {
    const el = $("#yyCampaignId");
    return el ? String(el.value || "").trim() : "";
  }

  async function switchToAdvanced() {
    try {
      const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
      const editorHtml = ed ? (ed.getContent({ format: "html" }) || "") : "";

      const data = await postJson("/panel/campaigns/campaigns/letter/_extract-content/", {
        editor_html: editorHtml || "",
      });
      if (!data || !data.ok) return;

      showAdvancedMode();

      if (typeof window.yyCampEnsureCodeMirror === "function") window.yyCampEnsureCodeMirror();
      if (typeof window.yyCampAdvSet === "function") window.yyCampAdvSet(normalizeTabsTo2Spaces(data.content_html || ""));

      if (typeof window.yyCampAdvRefresh === "function") {
        setTimeout(() => window.yyCampAdvRefresh(), 0);
      }
    } catch (e) {}
  }

  async function switchToUser() {
    try {
      if (typeof window.yyCampEnsureCodeMirror === "function") window.yyCampEnsureCodeMirror();
      const content = normalizeTabsTo2Spaces(
        typeof window.yyCampAdvGetHtml === "function" ? window.yyCampAdvGetHtml() : ""
      );

      const id = getCampaignId();
      if (!id) return;

      const data = await postJson("/panel/campaigns/campaigns/letter/_render-editor-html/", {
        id: id,
        content_html: content || "",
      });
      if (!data || !data.ok) return;

      const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
      if (ed) ed.setContent(data.editor_html || "");

      showUserMode();
    } catch (e) {}
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
