// FILE: web/static/js/campaign_templates/campaign_letters/tinymce_config.js
// DATE: 2026-01-27
// PURPOSE: Tiny config для письма: детерминированное Enter/Paste + динамические кнопки вставки HTML.
// CHANGE:
// - Link: как в конфиге шаблонов — всегда открываем НАШ диалог "Link" (toolbar + контекст/панель через mceLink перехват).
//   Protocol=(none/https/mailto/tel/WhatsApp/Telegram) + Address + Text; Remove link слева с иконкой unlink (только при edit).
//   WhatsApp/Telegram — префиксы https://wa.me/ и https://t.me/ (не протоколы).
// - Добавлена кнопка "•" (вставляет &bull;&nbsp;&nbsp; в курсор).
// - Сохранено: Paste text-><p>, Enter-нормализация <br><br> в split <p>, caret-фиксы, init-css, yyCampInitButtons.

(function () {
  "use strict";

  if (!window.tinymce) return;

  const $ = (s) => document.querySelector(s);

  /* ===== Link prefixes (same as templates config) ===== */

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
                maximized: true,
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

  /* ===== init inputs ===== */

  function readInitCss() {
    const ta = $("#yyInitCss");
    return ta ? String(ta.value || "") : "";
  }

  function readInitButtons() {
    const obj = window.yyCampInitButtons;
    if (!obj || typeof obj !== "object") return [];

    const out = [];
    let i = 0;
    for (const k of Object.keys(obj)) {
      if (i >= 24) break; // safety
      const text = String(k || "").trim();
      if (!text) continue;
      out.push({ id: "yyInsBtn" + String(i + 1), text, html: String(obj[k] || "") });
      i += 1;
    }
    return out;
  }

  function buildTinyConfig() {
    const insButtons = readInitButtons();
    const extraToolbar = insButtons.length ? " | " + insButtons.map((b) => b.id).join(" ") : "";

    return {
      selector: "#yyTinyEditor",
      inline: false,

      menubar: false,
      branding: false,
      statusbar: false,

      plugins: "link",
      toolbar: "undo redo | bold italic | yyBullet | link" + extraToolbar,
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

      setup(editor) {
        // dynamic "insert HTML" buttons from GlobalTemplate.buttons
        try {
          (insButtons || []).forEach((b) => {
            editor.ui.registry.addButton(b.id, {
              text: b.text,
              onAction: () => {
                try {
                  editor.insertContent(String(b.html || ""));
                } catch (_) {}
              },
            });
          });
        } catch (_) {}

        // bullet button
        try {
          editor.ui.registry.addButton("yyBullet", {
            text: "●",
            tooltip: "Bullet",
            onAction: () => {
              try {
                editor.insertContent("&bull;&nbsp;&nbsp;");
              } catch (_) {}
            },
          });
        } catch (_) {}

        // Link dialog override (toolbar + context/panel)
        editor.on("BeforeExecCommand", function (e) {
          if (e && e.command === "mceLink") {
            e.preventDefault();
            openLinkDialog(editor);
          }
        });

        let enterArmed = false;
        let postScheduled = false;

        /* ===== helpers ===== */

        const isRealBR = (n) =>
          n &&
          n.nodeType === 1 &&
          n.nodeName === "BR" &&
          !n.hasAttribute("data-mce-bogus");

        const isEmptyText = (n) =>
          n && n.nodeType === 3 && !String(n.nodeValue || "").trim();

        function nextMeaningful(n) {
          let x = n ? n.nextSibling : null;
          while (x && isEmptyText(x)) x = x.nextSibling;
          return x;
        }

        function findDoubleBr(p) {
          // Ищем паттерн: <br> [пустые text]* <br>
          for (let n = p.firstChild; n; n = n.nextSibling) {
            if (!isRealBR(n)) continue;
            const n2 = nextMeaningful(n);
            if (isRealBR(n2)) return { br1: n, br2: n2 };
          }
          return null;
        }

        function splitP(p) {
          const hit = findDoubleBr(p);
          if (!hit) return null;

          const { br1, br2 } = hit;
          const newP = editor.dom.create("p");

          // Переносим всё после второго <br> в новый <p>
          let n = br2.nextSibling;
          while (n) {
            const next = n.nextSibling;
            newP.appendChild(n);
            n = next;
          }

          // Удаляем br1, пустые text между, br2
          let x = br1;
          while (x && x !== br2) {
            const next = x.nextSibling;
            if (x === br1 || isEmptyText(x)) x.remove();
            x = next;
          }
          if (br2.parentNode === p) br2.remove();

          if (!newP.firstChild) newP.innerHTML = "&nbsp;";

          editor.dom.insertAfter(newP, p);
          return newP;
        }

        function normalizeAllP() {
          const body = editor.getBody();
          if (!body) return null;

          const ps = Array.from(body.querySelectorAll("p"));
          let lastCreated = null;

          // Обрабатываем все <p>, и зацикливаемся, т.к. может быть <br><br><br>
          for (let i = 0; i < ps.length; i++) {
            let p = ps[i];
            while (true) {
              const created = splitP(p);
              if (!created) break;
              lastCreated = created;
              // новый <p> тоже может содержать двойные <br> — добавляем в очередь
              ps.splice(i + 1, 0, created);
              p = created;
            }
          }

          return lastCreated;
        }

        function pokeTinyCaretState() {
          // важно: сбрасывает внутреннее caret-состояние Tiny, иначе второй <br> может блокироваться
          editor.execCommand("mceInsertContent", false, "");
        }

        function setCaretToStart(el) {
          editor.selection.select(el, true);
          editor.selection.collapse(true);
          pokeTinyCaretState();
        }

        function moveCaretBeforeLastBR() {
          const p = editor.dom.getParent(editor.selection.getNode(), "p");
          if (!p) return false;

          for (let i = p.childNodes.length - 1; i >= 0; i--) {
            const n = p.childNodes[i];
            if (isRealBR(n)) {
              const r = editor.dom.createRng();
              r.setStartBefore(n);
              r.collapse(true);
              editor.selection.setRng(r);
              pokeTinyCaretState();
              return true;
            }
          }
          return false;
        }

        function postProcessAfterEnter() {
          if (postScheduled) return;
          postScheduled = true;

          setTimeout(() => {
            postScheduled = false;
            if (!enterArmed) return;
            enterArmed = false;

            editor.undoManager.transact(() => {
              const lastP = normalizeAllP();
              if (lastP) {
                setCaretToStart(lastP);
              } else {
                moveCaretBeforeLastBR();
              }
            });
          }, 0);
        }

        /* ===== paste: text -> <p> ===== */

        editor.on("paste", function (e) {
          e.preventDefault();
          const cd = e.clipboardData || window.clipboardData;
          const text = cd ? String(cd.getData("text/plain") || "") : "";
          if (!text) return;

          const lines = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n");
          const html = lines
            .map((l) => {
              const safe = editor.dom.encode(l);
              return safe ? `<p>${safe}</p>` : "<p>&nbsp;</p>";
            })
            .join("");

          editor.undoManager.transact(() => editor.insertContent(html));
        });

        /* ===== enter tracking ===== */

        editor.on("keydown", function (e) {
          if (
            e.key === "Enter" &&
            !e.shiftKey &&
            !e.altKey &&
            !e.ctrlKey &&
            !e.metaKey
          ) {
            enterArmed = true;
          }
        });

        // Tiny сначала сам вставляет <br>, потом мы в post-tick нормализуем DOM
        editor.on("input", postProcessAfterEnter);
        editor.on("keyup", postProcessAfterEnter);

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
