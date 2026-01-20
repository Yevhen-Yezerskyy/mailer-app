// FILE: web/static/js/campaign_templates/campaign_letters/tinymce_config.js
// DATE: 2026-01-20
// PURPOSE: Tiny config для письма: грузим CSS из yyInitCss, Tiny содержит ВИЗУАЛЬНЫЙ HTML (template+content).
// CHANGE: content_style берём из textarea yyInitCss.

(function () {
  "use strict";

  if (!window.tinymce) return;

  const $ = (s) => document.querySelector(s);

  function readInitCss() {
    const ta = $("#yyInitCss");
    return ta ? String(ta.value || "") : "";
  }

  function buildTinyConfig() {
    return {
      selector: "#yyTinyEditor",
      inline: false,

      menubar: false,
      branding: false,
      statusbar: false,

      plugins: "link",
      toolbar: "undo redo | bold italic | link",
      link_default_target: "_blank",

      newline_behavior: "linebreak",
      forced_root_block: "p",

      height: 700,
      min_height: 700,
      resize: false,

      content_css: false,
      content_style: readInitCss() || "",
      valid_elements: "*[*]",

      skin: "tinymce-5",
      skin_url: "/static/vendor/tinymce/skins/ui/tinymce-5",

      icons: "default",
      icons_url: "/static/vendor/tinymce/icons/default/icons.min.js",

      setup: function (editor) {
        editor.on("init", function () {
          if (typeof window.yyCampRuntimeOnEditorInit === "function") {
            window.yyCampRuntimeOnEditorInit(editor);
          }
        });
      },
    };
  }

  window.yyCampTinyBuildConfig = buildTinyConfig;
})();
