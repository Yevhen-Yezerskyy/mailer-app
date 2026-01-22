// FILE: web/static/js/campaign_templates/campaign_letters/tinymce_config.js
// DATE: 2026-01-22
// PURPOSE: Tiny config для письма: исходный конфиг + детерминированное поведение Enter/Paste.
// CHANGE:
// - Paste: только plain text; каждая строка -> <p>...</p>.
// - После Enter: глобально по DOM меняем <br><br> внутри <p> на split <p> -> <p></p><p></p>.
//   Каретка ставится в начало ПОСЛЕДНЕГО созданного <p>.
// - Если split не было: каретка ставится ПЕРЕД последним <br> в текущем <p>.
// - После программного перемещения каретки: дергаем Tiny (mceInsertContent пустым) чтобы сбросить caret-state,
//   иначе второй <br> может не разрешаться до ручного движения курсора.
// - NEW: динамические кнопки вставки HTML из window.yyCampInitButtons (ключ=название, значение=HTML).

(function () {
  "use strict";

  if (!window.tinymce) return;

  const $ = (s) => document.querySelector(s);

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
      toolbar: "undo redo | bold italic | link" + extraToolbar,
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
