// FILE: web/static/js/campaign_templates/tinymce_config.js
// DATE: 2026-01-17
// PURPOSE: TinyMCE config для Campaign Templates (user-mode).
// CHANGE:
// - сохранён исходный toolbar/plugins (как у тебя “работает”)
// - убран resize="vertical" (ошибка Invalid value...)
// - отключены content_css/content_style, чтобы Tiny НЕ влиял стилями на внутренний HTML
// - остальное без изменений

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

      forced_root_block: "p",

      height: 700,
      min_height: 700,

      // FIX: у твоего TinyMCE не принимается "vertical"
      // (оставляем без ресайза, чтобы не было warning)
      resize: false,

      // FIX: Tiny CSS НЕ должен влиять на контент (у тебя свой yyLiveCss)
      content_css: false,
      content_style: "",

      valid_elements: "*[*]",

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
