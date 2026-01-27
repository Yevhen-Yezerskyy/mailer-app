// FILE: web/static/js/campaign_templates/tinymce_config.js
// DATE: 2026-01-27
// PURPOSE: TinyMCE config для шаблонов писем.
// CHANGE:
// - Всегда открываем НАШ диалог "Link" вместо дефолтного (toolbar link + контекстное меню/панель).
// - Диалог: Protocol (узко) + Address (широко) + Text; Remove link слева с иконкой unlink (только при edit ссылки).
// - WhatsApp/Telegram: это префиксы https://wa.me/ и https://t.me/ (не протоколы).
// - Paste: только plain text.
// - Никакой валидации/нормализации address — только склейка prefix + address.

(function () {
  "use strict";

  if (!window.tinymce) return;

  const PREFIXES = {
    "": "",
    "https://": "https://",
    "mailto:": "mailto:",
    "tel:": "tel:",
    wa: "https://wa.me/",
    tg: "https://t.me/",
  };

  const PARSE_PREFIXES = [
    { key: "wa", prefix: "https://wa.me/" },
    { key: "tg", prefix: "https://t.me/" },
    { key: "https://", prefix: "https://" },
    { key: "mailto:", prefix: "mailto:" },
    { key: "tel:", prefix: "tel:" },
  ];

  function parseHref(href) {
    href = String(href || "");
    for (const it of PARSE_PREFIXES) {
      if (href.startsWith(it.prefix)) {
        return { protoKey: it.key, address: href.slice(it.prefix.length) };
      }
    }
    return { protoKey: "", address: href };
  }

  function currentLinkNode(editor) {
    const n = editor.selection.getNode();
    if (!n) return null;
    if (n.nodeName === "A") return n;
    return editor.dom.getParent(n, "a");
  }

  function openLinkDialog(editor) {
    const linkNode = currentLinkNode(editor);

    const href = linkNode ? editor.dom.getAttrib(linkNode, "href") : "";
    const parsed = parseHref(href);

    const selectedText = editor.selection.getContent({ format: "text" }) || "";
    const existingText = linkNode ? (linkNode.textContent || "") : "";
    const initialText = existingText || selectedText;

    editor.windowManager.open({
      title: "Link",
      body: {
        type: "panel",
        items: [
          {
            type: "bar",
            items: [
              {
                type: "selectbox",
                name: "protoKey",
                label: "Protocol",
                items: [
                  { text: "(none)", value: "" },
                  { text: "https://", value: "https://" },
                  { text: "mailto:", value: "mailto:" },
                  { text: "tel:", value: "tel:" },
                  { text: "WhatsApp", value: "wa" },
                  { text: "Telegram", value: "tg" },
                ],
              },
              {
                type: "input",
                name: "address",
                label: "Address",
                maximized: true, // Address шире, Protocol уже
              },
            ],
          },
          {
            type: "input",
            name: "text",
            label: "Text",
          },
        ],
      },
      initialData: {
        protoKey: parsed.protoKey,
        address: parsed.address,
        text: initialText,
      },
      buttons: [
        linkNode
          ? { type: "custom", name: "unlink", text: "Remove link", icon: "unlink", align: "start" }
          : null,
        { type: "cancel", text: "Cancel" },
        { type: "submit", text: "Save", primary: true },
      ].filter(Boolean),

      onAction(api, details) {
        if (details.name === "unlink") {
          editor.execCommand("unlink");
          api.close();
        }
      },

      onSubmit(api) {
        const d = api.getData() || {};
        const protoKey = String(d.protoKey || "");
        const address = String(d.address || "");
        const text = String(d.text || "");

        const prefix = Object.prototype.hasOwnProperty.call(PREFIXES, protoKey) ? PREFIXES[protoKey] : "";
        const newHref = prefix ? prefix + address : address;

        const ln = currentLinkNode(editor);
        if (ln) {
          editor.dom.setAttrib(ln, "href", newHref);
          if (text) ln.textContent = text;
        } else {
          editor.execCommand("mceInsertLink", false, newHref);
          if (text) {
            const n = editor.selection.getNode();
            if (n && n.nodeName === "A") n.textContent = text;
          }
        }

        api.close();
      },
    });
  }

  function buildTinyConfig() {
    return {
      selector: "#yyTinyEditor",
      inline: false,

      menubar: false,
      branding: false,
      statusbar: false,

      plugins: "link paste",
      toolbar: "undo redo | bold italic | link", // оставляем стандартную кнопку link, но перехватываем команду

      link_default_target: "_blank",

      // Paste => plain text
      paste_as_text: true,
      paste_text_sticky: true,
      paste_text_sticky_default: true,
      paste_data_images: false,

      // Enter => <br>
      newline_behavior: "linebreak",
      forced_root_block: "p",

      height: 700,
      min_height: 700,
      resize: false,

      content_css: false,
      content_style: "",

      valid_elements: "*[*]",

      entity_encoding: "raw",
      encoding: "utf-8",

      skin: "tinymce-5",
      skin_url: "/static/vendor/tinymce/skins/ui/tinymce-5",

      icons: "default",
      icons_url: "/static/vendor/tinymce/icons/default/icons.min.js",

      setup: function (editor) {
        // Жёстко перехватываем любой вызов дефолтного link-диалога
        editor.on("BeforeExecCommand", function (e) {
          if (e && e.command === "mceLink") {
            e.preventDefault();
            openLinkDialog(editor);
          }
        });

        // На init оставляем твой хук
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
