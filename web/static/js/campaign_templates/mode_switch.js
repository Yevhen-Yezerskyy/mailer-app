// FILE: web/static/js/campaign_templates/mode_switch.js
// DATE: 2026-01-17
// PURPOSE: Переключение user<->advanced для Templates (TinyMCE <-> CodeMirror) через сервер.
// CHANGE: Восстановлены глобальные функции yyTplSwitchToAdvanced/yyTplSwitchToUser (кнопка ADVANNCED снова работает).
//         Используем: yyTplEnsureCodeMirror/yyTplAdvSet/yyTplAdvGetHtml/yyTplAdvGetCss, yyTplGetCss/yyTplSetCss.

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

  
  // FILE: web/static/js/campaign_templates/mode_switch.js
  // DATE: 2026-01-17
  // PURPOSE: Переключение user<->advanced.
  // CHANGE: switchToAdvanced больше НЕ падает/не выходит при пустом HTML (в edit с пустым контентом кнопка обязана работать).

  async function switchToAdvanced() {
    try {
      const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
      const editorHtml = ed ? (ed.getContent({ format: "html" }) || "") : "";

      // ВАЖНО: advanced должен открываться даже если editorHtml пустой
      const data = await postJson("/panel/campaigns/templates/_parse-editor-html/", {
        editor_html: editorHtml,
      });
      if (!data || !data.ok) return;

      // 1) show advanced first (CM не любит hidden)
      showAdvancedMode();

      // 2) ensure CM
      if (typeof window.yyTplEnsureCodeMirror === "function") window.yyTplEnsureCodeMirror();

      const tpl = normalizeTabsTo2Spaces((data.template_html || "").trim());
      const css0 = typeof window.yyTplGetCss === "function" ? window.yyTplGetCss() : "";
      const css = normalizeTabsTo2Spaces(formatCss(css0 || ""));

      if (typeof window.yyTplAdvSet === "function") window.yyTplAdvSet(tpl, css);

      if (typeof window.yyTplAdvRefresh === "function") {
        setTimeout(() => window.yyTplAdvRefresh(), 0);
      }
    } catch (e) {}
  }


  async function switchToUser() {
    try {
      // 1) ensure CM exists
      if (typeof window.yyTplEnsureCodeMirror === "function") window.yyTplEnsureCodeMirror();

      // 2) get template_html + css from CM
      const tpl = normalizeTabsTo2Spaces(
        typeof window.yyTplAdvGetHtml === "function" ? window.yyTplAdvGetHtml() : ""
      );
      const css = normalizeTabsTo2Spaces(
        typeof window.yyTplAdvGetCss === "function" ? window.yyTplAdvGetCss() : ""
      );

      // 3) render editor html with demo content
      const data = await postJson("/panel/campaigns/templates/_render-editor-html/", {
        template_html: tpl || "",
      });
      if (!data || !data.ok) return;

      // 4) apply css to Tiny live store
      if (typeof window.yyTplSetCss === "function") window.yyTplSetCss(css || "");

      // 5) set Tiny content
      const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
      if (ed) ed.setContent(data.editor_html || "");

      // 6) show UI
      showUserMode();
    } catch (e) {}
  }

  // globals for buttons
  window.yyTplSwitchToAdvanced = switchToAdvanced;
  window.yyTplSwitchToUser = switchToUser;

  // default (page load)
  function init() {
    // если вдруг шаблон отрендерился в advanced — покажем user по умолчанию
    showUserMode();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
