// FILE: web/static/js/campaign_templates/campaign_letters/tinymce.js
// DATE: 2026-01-22
// PURPOSE: Tiny runtime: init html (visual template+content) + helpers extract/compose content.
// CHANGE:
// - Before Tiny init: fetch GlobalTemplate.buttons by template_html (id-<N> in first tag) and expose as window.yyCampInitButtons.

(function () {
  "use strict";

  const $ = (s) => document.querySelector(s);

  const PLACEHOLDER = "{{ ..content.. }}";
  const WRAP_CLASS = "yy_content_wrap";

  function findWrapperSpan(s) {
    s = s || "";
    if (!s) return null;

    const reStart = new RegExp(
      '<table\\b[^>]*\\bclass\\s*=\\s*["\\\'][^"\\\']*\\b' + WRAP_CLASS + '\\b[^"\\\']*["\\\'][^>]*>',
      "i"
    );
    const m = reStart.exec(s);
    if (!m) return null;

    const start = m.index;
    let pos = start + m[0].length;

    const tokenRe = /<table\b[^>]*>|<\/table\s*>/gi;
    tokenRe.lastIndex = pos;

    let depth = 1;
    while (true) {
      const t = tokenRe.exec(s);
      if (!t) return null;
      const tok = String(t[0] || "").toLowerCase();
      if (tok.startsWith("</table")) {
        depth -= 1;
        if (depth === 0) {
          return [start, tokenRe.lastIndex];
        }
      } else {
        depth += 1;
      }
    }
  }

  function unwrapContent(editorHtml) {
    const span = findWrapperSpan(editorHtml || "");
    if (!span) return "";
    const wrapper = (editorHtml || "").slice(span[0], span[1]);
    const m = /<td\b[^>]*>([\s\S]*?)<\/td>/i.exec(wrapper);
    return m ? (m[1] || "") : "";
  }

  function wrapContent(innerHtml) {
    return '<table class="' + WRAP_CLASS + '"><tr><td>' + (innerHtml || "") + "</td></tr></table>";
  }

  function stripTinyEditClasses(html) {
    return String(html || "").replace(/\b(mceNonEditable|mceEditable)\b/g, "").replace(/\s{2,}/g, " ");
  }

  function forceTinyClassOnTags(html, cls) {
    // rough but ok for sanitized email html
    return String(html || "").replace(/<([a-zA-Z][a-zA-Z0-9:_-]*)([^<>]*?)>/g, function (full, tag, attrs) {
      const name = String(tag || "").toLowerCase();
      if (name === "script" || name === "style" || name === "meta" || name === "br" || name === "hr") return full;

      const a = String(attrs || "");
      const m = /\bclass\s*=\s*(["'])([\s\S]*?)\1/i.exec(a);
      let classes = [];
      if (m) classes = String(m[2] || "").split(/\s+/).filter(Boolean);

      // drop both, add cls
      classes = classes.filter((c) => c !== "mceNonEditable" && c !== "mceEditable");
      if (classes.indexOf(cls) === -1) classes.push(cls);

      const newClass = ' class="' + classes.join(" ").trim() + '"';
      if (m) {
        const before = a.slice(0, m.index);
        const after = a.slice(m.index + m[0].length);
        return "<" + tag + before + newClass + after + ">";
      }
      return "<" + tag + newClass + a + ">";
    });
  }

  function buildVisualFromContent(contentHtml) {
    const taTpl = $("#yyTemplateHtml");
    const tpl = taTpl ? String(taTpl.value || "") : "";
    if (!tpl) return String(contentHtml || "");

    const raw = tpl.replace(PLACEHOLDER, wrapContent(contentHtml || ""), 1);

    // apply editability: outside wrapper non-edit, inside wrapper edit
    const span = findWrapperSpan(raw);
    if (!span) return forceTinyClassOnTags(raw, "mceEditable");

    const before = raw.slice(0, span[0]);
    const mid = raw.slice(span[0], span[1]);
    const after = raw.slice(span[1]);

    return (
      forceTinyClassOnTags(before, "mceNonEditable") +
      forceTinyClassOnTags(mid, "mceEditable") +
      forceTinyClassOnTags(after, "mceNonEditable")
    );
  }

  function initContent(editor) {
    const src = $("#yyInitHtml");
    const html = src ? (src.value || "") : "";
    try {
      editor.setContent(html || "");
    } catch (_) {}
  }

  window.yyCampRuntimeOnEditorInit = function (editor) {
    initContent(editor);
  };

  window.YYCampaignLetterTiny = {
    getEditorHtml: function () {
      try {
        const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
        return ed ? (ed.getContent({ format: "html" }) || "") : "";
      } catch (_) {
        return "";
      }
    },
    getContentHtml: function () {
      try {
        const editorHtml = this.getEditorHtml();
        return unwrapContent(editorHtml || "") || "";
      } catch (_) {
        return "";
      }
    },
    setFromContentHtml: function (contentHtml) {
      try {
        const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
        if (!ed) return;
        const visual = buildVisualFromContent(stripTinyEditClasses(contentHtml || ""));
        ed.setContent(visual || "");
      } catch (_) {}
    },
  };

  function init() {
    const ta = $("#yyTinyEditor");
    if (!ta) return;
    if (!window.tinymce || typeof window.yyCampTinyBuildConfig !== "function") return;
    (async () => {
      try {
        const taTpl = $("#yyTemplateHtml");
        const tpl = taTpl ? String(taTpl.value || "") : "";

        // by default: no buttons
        window.yyCampInitButtons = {};

        if (tpl) {
          const r = await fetch("/panel/campaigns/campaigns/letter/_buttons-by-template/", {
            method: "POST",
            credentials: "same-origin",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ template_html: tpl }),
          });
          if (r.ok) {
            const data = await r.json();
            if (data && data.ok && data.buttons && typeof data.buttons === "object") {
              window.yyCampInitButtons = data.buttons;
            }
          }
        }
      } catch (_) {
        window.yyCampInitButtons = {};
      }

      try {
        window.tinymce.init(window.yyCampTinyBuildConfig());
      } catch (_) {}
    })();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
