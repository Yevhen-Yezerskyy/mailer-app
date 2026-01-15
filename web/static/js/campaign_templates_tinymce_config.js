// FILE: web/static/js/campaign_templates_tinymce_config.js  (обновлено — 2026-01-15)
// CHANGE: фикс высоты: min 800px + внутренний скролл; отключён autoresize.

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

      // !!! autoresize убрать !!!
      plugins: "link",
      toolbar: "undo redo | bold italic | link",
      link_default_target: "_blank",

      forced_root_block: "p",

      // Высота: минимум 800, дальше — скролл в контенте
      height: 700,
      min_height: 700,
      resize: "vertical",

      // чтобы был скролл в области редактирования
      content_style: `
        html { padding: 0 !important; margin: 0 !important; }
        body { padding: 4px !important; margin: 0 !important; }
        body { min-height: 700px !important; overflow-y: auto !important; }
      `,

      valid_elements: "*[*]",

      skin: "tinymce-5",
      skin_url: "/static/vendor/tinymce/skins/ui/tinymce-5",
      content_css: "/static/vendor/tinymce/skins/content/tinymce-5/content.inline.min.css",

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
