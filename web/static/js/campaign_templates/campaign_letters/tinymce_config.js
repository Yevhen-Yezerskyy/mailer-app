// FILE: web/static/js/campaign_letters/tinymce_config.js
// DATE: 2026-01-19
// PURPOSE: TinyMCE config для письма кампании (редактируем только внутренний HTML).
// CHANGE: (new) setup вызывает yyCampRuntimeOnEditorInit.

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

      newline_behavior: "linebreak",
      forced_root_block: "p",

      height: 700,
      min_height: 700,
      resize: false,

      content_css: false,
      content_style: "",
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
