// FILE: web/static/js/campaign_templates_tinymce_config.js
// DATE: 2026-01-14
// PURPOSE: TinyMCE init-config (user-mode only).
// CHANGE: Вынесено отдельно от runtime. Только конфиг и tinymce.init(), без загрузки/сохранения/стилей.

(function () {
  "use strict";

  if (!window.tinymce) return;

  function buildTinyConfig(editorEl) {
    return {
      target: editorEl,
      inline: true,
      menubar: false,
      branding: false,
      statusbar: false,

      plugins: "link lists autoresize",
      toolbar:
        "undo redo | bold italic underline | alignleft aligncenter alignright | bullist numlist | link",
      link_default_target: "_blank",

      forced_root_block: "p",
      autoresize_bottom_margin: 16,

      // Разрешаем почти всё на уровне редактора; финальная очистка — только сервером sanitize_stored_html().
      valid_elements: "*[*]",

      // Хук для runtime: пусть он вешается на init и делает loadExisting().
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
