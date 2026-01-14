// FILE: web/static/js/campaign_templates_editor.js  (обновлено)
/// DATE: 2026-01-14
// PURPOSE: Минимальный JS (без парсинга HTML/CSS/JSON):
//          - User-mode: запросить у сервера HTML для Quill (уже со style-tag + demo)
//          - Save: отправить HTML из Quill на normalize -> получить чистый template_html + styles JSON -> записать в hidden -> submit
//          - Preview(2 кнопки): сервер рисует модалку (style_tag / inline)

(function () {
  "use strict";

  const $ = (s) => document.querySelector(s);

  function findFieldByName(name) {
    return document.querySelector(`[name="${name}"]`);
  }

  async function postForm(url, dataObj) {
    const fd = new FormData();
    for (const k of Object.keys(dataObj || {})) fd.append(k, dataObj[k] ?? "");
    const r = await fetch(url, { method: "POST", credentials: "same-origin", body: fd });
    return r;
  }

  function isVisible(el) {
    return el && !el.classList.contains("hidden");
  }

  function init() {
    const form = $("#yyTplForm");
    if (!form) return;

    const btnUser = $("#yyModeUser");
    const btnAdv = $("#yyModeAdv");

    const btnPreviewCss = $("#yyPreviewBtn"); // оставь как есть (style_tag)
    const btnPreviewInline = $("#yyPreviewInlineBtn"); // добавишь в template: вторая кнопка

    const boxUser = $("#yyModeUserBox");
    const boxAdv = $("#yyModeAdvBox");

    const quillHost = $("#yyQuillEditor");

    const cmHtmlHost = $("#yyCMHtmlHost");
    const cmJsonHost = $("#yyCMJsonHost");

    const fieldHtml = findFieldByName("template_html");
    const fieldStyles = findFieldByName("styles");
    if (!fieldHtml || !fieldStyles) return;

    let quill = null;
    let cmHtml = null;
    let cmJson = null;

    function ensureQuill() {
      if (quill) return;
      if (!window.Quill) return;
      quill = new window.Quill(quillHost, {
        theme: "snow",
        modules: { toolbar: true },
      });
    }

    function ensureCodeMirror() {
      if (cmHtml && cmJson) return;
      if (!window.CodeMirror) return;

      cmHtml = window.CodeMirror(cmHtmlHost, {
        value: fieldHtml.value || "",
        mode: "htmlmixed",
        lineNumbers: true,
        tabSize: 2,
        indentUnit: 2,
      });

      cmJson = window.CodeMirror(cmJsonHost, {
        value: fieldStyles.value || "",
        mode: { name: "javascript", json: true },
        lineNumbers: true,
        tabSize: 2,
        indentUnit: 2,
      });
    }

    function setMode(mode) {
      if (mode === "adv") {
        ensureCodeMirror();
        if (cmHtml) cmHtml.setValue(fieldHtml.value || "");
        if (cmJson) cmJson.setValue(fieldStyles.value || "");
        boxUser && boxUser.classList.add("hidden");
        boxAdv && boxAdv.classList.remove("hidden");
        return;
      }

      // user
      boxAdv && boxAdv.classList.add("hidden");
      boxUser && boxUser.classList.remove("hidden");
      renderUserToQuill();
    }

    async function renderUserToQuill() {
      ensureQuill();
      if (!quill) return;

      // берём исходник из hidden (или из CM, если adv открыт)
      if (isVisible(boxAdv) && cmHtml && cmJson) {
        fieldHtml.value = cmHtml.getValue() || "";
        fieldStyles.value = cmJson.getValue() || "";
      }

      const r = await postForm("/panel/campaigns/templates/_render_user/", {
        template_html: fieldHtml.value || "",
        styles: fieldStyles.value || "{}",
      });

      if (!r.ok) return;
      const j = await r.json();
      quill.root.innerHTML = (j && j.html) ? j.html : "";
    }

    async function normalizeAndWriteHidden() {
      // source html: из Quill (user) или из CM (adv)
      if (isVisible(boxAdv) && cmHtml && cmJson) {
        fieldHtml.value = cmHtml.getValue() || "";
        fieldStyles.value = cmJson.getValue() || "";
        return true;
      }

      ensureQuill();
      if (!quill) return false;

      const editorHtml = quill.root.innerHTML || "";

      const r = await postForm("/panel/campaigns/templates/_normalize/", { editor_html: editorHtml });
      if (!r.ok) return false;

      const j = await r.json();
      fieldHtml.value = (j && j.template_html) ? j.template_html : "";
      fieldStyles.value = (j && j.styles) ? j.styles : "{}";
      return true;
    }

    async function openPreview(mode) {
      // всегда превью строим из “хранимого” вида: user -> normalize, adv -> прямые поля
      const ok = await normalizeAndWriteHidden();
      if (!ok) return;

      const r = await postForm("/panel/campaigns/templates/_preview/", {
        template_html: fieldHtml.value || "",
        styles: fieldStyles.value || "{}",
        mode: mode || "style_tag",
      });

      if (!r.ok) return;
      const html = await r.text();
      if (window.YYModal && window.YYModal.open) window.YYModal.open("text=" + html);
    }

    // Кнопки
    if (btnUser) btnUser.addEventListener("click", () => setMode("user"));
    if (btnAdv) btnAdv.addEventListener("click", () => setMode("adv"));

    if (btnPreviewCss) btnPreviewCss.addEventListener("click", () => openPreview("style_tag"));
    if (btnPreviewInline) btnPreviewInline.addEventListener("click", () => openPreview("inline"));

    // submit: user-mode -> normalize на сервере и только потом submit
    form.addEventListener("submit", async (e) => {
      // close/cancel не трогаем
      const active = document.activeElement;
      const isClose = active && active.name === "action" && active.value === "close";
      if (isClose) return;

      const ok = await normalizeAndWriteHidden();
      if (!ok) {
        // если нормализация провалилась — не ломаем submit, просто дадим Django валидацию
        return;
      }
    });

    // default
    setMode("user");
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
