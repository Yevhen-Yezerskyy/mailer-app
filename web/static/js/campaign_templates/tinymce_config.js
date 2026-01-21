// FILE: web/static/js/campaign_templates/tinymce_config.js
// DATE: 2026-01-21
// PURPOSE: TinyMCE config для шаблонов писем.
// CHANGE:
// - Отключена HTML-entity подмена (ö -> &ouml;).
// - Явно UTF-8. Без влияния на email/preview/reverse-vars.

(function () {
  "use strict";

  if (!window.tinymce) return;

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

      // Enter => <br>
      newline_behavior: "linebreak",
      forced_root_block: "p",

      height: 700,
      min_height: 700,
      resize: false,

      content_css: false,
      content_style: "",

      valid_elements: "*[*]",

      // КЛЮЧЕВОЕ:
      entity_encoding: "raw",
      encoding: "utf-8",

      skin: "tinymce-5",
      skin_url: "/static/vendor/tinymce/skins/ui/tinymce-5",

      icons: "default",
      icons_url: "/static/vendor/tinymce/icons/default/icons.min.js",

      setup: function (editor) {
        editor.on("init", function () {
          if (typeof window.yyTplRuntimeOnEditorInit === "function") {
            window.yyTplRuntimeOnEditorInit(editor);
          }
        });
      },
    };
  }

  window.yyTinyBuildConfig = buildTinyConfig;
})();
