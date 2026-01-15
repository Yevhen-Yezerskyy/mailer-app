// FILE: web/static/js/campaign_templates_editor.js  (обновлено — 2026-01-15)
// CHANGE: live-CSS теперь инжектится ВНУТРЬ iframe через Tiny API (а не <style> в основном документе).
//         Убран reliance на contenteditable div — работаем с tinymce.get("yyTinyEditor").

(function () {
  "use strict";

  const $ = (s) => document.querySelector(s);

  function getParam(name) {
    try {
      return new URLSearchParams(window.location.search).get(name);
    } catch {
      return null;
    }
  }

  async function fetchText(url) {
    const r = await fetch(url, { credentials: "same-origin" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.text();
  }

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

  function formatCss(cssText) {
    const txt = (cssText || "").trim();
    if (!txt) return "";

    const out = [];
    const re = /([\s\S]*?)\{([\s\S]*?)\}/g;
    let m;
    while ((m = re.exec(txt))) {
      const sel = (m[1] || "").trim();
      const body = (m[2] || "").trim();
      if (!sel) continue;

      const props = [];
      body.split(";").forEach((p) => {
        const s = (p || "").trim();
        if (!s) return;
        const idx = s.indexOf(":");
        if (idx === -1) return;
        const k = s.slice(0, idx).trim();
        const v = s.slice(idx + 1).trim();
        if (!k) return;
        props.push(`  ${k}: ${v};`);
      });

      if (!props.length) continue;
      out.push(`${sel} {\n${props.join("\n")}\n}\n`);
    }

    return out.join("\n").trim() + "\n";
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

  function init() {
    const form = $("#yyTplForm");
    if (!form) return;

    const editorTextarea = $("#yyTinyEditor");
    const hiddenHtml = $("#yyEditorHtml");
    const hiddenCss = $("#yyCssText");
    if (!editorTextarea || !hiddenHtml || !hiddenCss) return;

    const advHtml = $("#yyAdvHtml");
    const advCss = $("#yyAdvCss");

    if (!window.tinymce || typeof window.yyTinyBuildConfig !== "function") return;

    let lastCssText = "";
    let currentEditor = null;

    function ensureIframeStyle(editor) {
      const doc = editor && editor.getDoc ? editor.getDoc() : null;
      if (!doc) return null;

      let el = doc.getElementById("yyLiveCss");
      if (el) return el;

      el = doc.createElement("style");
      el.id = "yyLiveCss";
      el.type = "text/css";
      (doc.head || doc.documentElement).appendChild(el);
      return el;
    }

    function applyCss(cssText) {
      lastCssText = (cssText || "").trim();

      // ВАЖНО: теперь CSS должен попадать внутрь iframe
      if (currentEditor) {
        const styleEl = ensureIframeStyle(currentEditor);
        if (styleEl) styleEl.textContent = lastCssText;
      }
    }

    window.yyTplSetCss = (css) => applyCss(css);
    window.yyTplGetCss = () => lastCssText;

    const state = (getParam("state") || "").trim();
    const uiId = (getParam("id") || "").trim();

    function loadExistingInto(editor) {
      if (!(state === "edit" && uiId)) {
        editor.setContent("");
        applyCss("");
        return;
      }

      const id = encodeURIComponent(uiId);
      const urlHtml = `/panel/campaigns/templates/_render-user-html/?id=${id}`;
      const urlCss = `/panel/campaigns/templates/_render-user-css/?id=${id}`;

      fetchText(urlHtml)
        .then((html) => {
          editor.setContent(html || "");
          return fetchText(urlCss);
        })
        .then((css) => applyCss(css))
        .catch(() => {});
    }

    window.yyTplRuntimeOnEditorInit = function (editor) {
      currentEditor = editor;

      // применим CSS повторно (если он пришёл до init)
      applyCss(lastCssText);

      loadExistingInto(editor);
    };

    window.tinymce.init(window.yyTinyBuildConfig());

    window.yyTplSwitchToAdvanced = async function () {
      try {
        const ed = window.tinymce.get("yyTinyEditor");
        const html = ed ? ed.getContent({ format: "html" }) : "";
        const data = await postJson("/panel/campaigns/templates/_parse-editor-html/", { editor_html: html || "" });
        if (!data || !data.ok) return;

        if (advHtml) advHtml.value = (data.template_html || "").trim();
        if (advCss) advCss.value = formatCss(lastCssText);
        showAdvancedMode();
      } catch (e) {}
    };

    window.yyTplSwitchToUser = async function () {
      try {
        const tpl = advHtml ? (advHtml.value || "") : "";
        const css = advCss ? (advCss.value || "") : "";

        const data = await postJson("/panel/campaigns/templates/_render-editor-html/", { template_html: tpl });
        if (!data || !data.ok) return;

        applyCss(css);

        const ed = window.tinymce.get("yyTinyEditor");
        if (ed) ed.setContent(data.editor_html || "");

        showUserMode();
      } catch (e) {}
    };

    showUserMode();

    form.addEventListener("submit", () => {
      const modeEl = $("#yyEditorMode");
      const mode = modeEl ? (modeEl.value || "user") : "user";

      if (mode === "advanced") {
        hiddenHtml.value = advHtml ? (advHtml.value || "") : "";
        hiddenCss.value = advCss ? (advCss.value || "") : "";
        return;
      }

      const ed = window.tinymce.get("yyTinyEditor");
      const html = ed ? ed.getContent({ format: "html" }) : "";
      hiddenHtml.value = html || "";
      hiddenCss.value = lastCssText || "";
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
