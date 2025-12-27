document.addEventListener("DOMContentLoaded", () => {

  /* ===============================
     SIDEBAR TOGGLE
     =============================== */
  const sidebar = document.getElementById("panel-sidebar");
  const btn = document.querySelector("[data-sidebar-toggle]");

  if (btn && sidebar) {
    btn.addEventListener("click", () => {
      sidebar.classList.toggle("hidden");
    });
  }

  /* ===============================
     DETAILS ICONS + ROTATE
     =============================== */
  document.querySelectorAll("details > summary").forEach(summary => {
    if (summary.querySelector("[data-icon]")) return;

    summary.insertAdjacentHTML(
      "afterbegin",
      `<svg data-icon class="ui-icon size-5 transition-transform">
         <use href="#icon-arrow-circle"></use>
       </svg>`
    );

    const details = summary.parentElement;
    const icon = summary.querySelector("[data-icon]");

    const sync = () => {
      icon.classList.toggle("rotate-90", details.hasAttribute("open"));
    };

    sync();
    details.addEventListener("toggle", sync);
  });

});


(function () {
  function showGlobalLoading() {
    const el = document.getElementById("yy-global-loading");
    if (!el) return;
    el.classList.remove("hidden");
    el.setAttribute("aria-hidden", "false");
  }

  // Важно: ловим именно "click" по кнопке/сабмиту, submit не трогаем.
  document.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-loading='1'], input[type='submit'][data-loading='1']");
    if (!btn) return;

    // чтобы не включать для обычных button без сабмита
    if (btn.tagName === "BUTTON" && (btn.type || "submit").toLowerCase() === "button") return;

    showGlobalLoading();
  }, true);

  // если браузер вернул страницу из bfcache (назад/вперёд) — спрячем оверлей
  window.addEventListener("pageshow", () => {
    const el = document.getElementById("yy-global-loading");
    if (!el) return;
    el.classList.add("hidden");
    el.setAttribute("aria-hidden", "true");
  });
})();

// FILE: web/static/js/script.js  (обновлено — 2025-12-23)
// PURPOSE: Простые табы по data-tabs/data-tab/data-panel. Кнопки переключаются через полную замену className (idle/active строки с пробелами).

document.addEventListener("click", function (e) {
  const btn = e.target.closest("[data-tab]");
  if (!btn) return;

  const box = btn.closest("[data-tabs]");
  if (!box) return;

  const tab = btn.dataset.tab;

  // buttons: set idle for all, active for clicked
  box.querySelectorAll("[data-tab]").forEach(b => {
    const idle = b.dataset.idleClass || "";
    if (idle) b.className = idle;
  });

  const active = btn.dataset.activeClass || "";
  if (active) btn.className = active;

  // panels: hide all, show target
  box.querySelectorAll("[data-panel]").forEach(p => {
    p.classList.add("hidden");
  });

  const panel = box.querySelector(`[data-panel="${tab}"]`);
  if (panel) panel.classList.remove("hidden");
});

// FILE: web/static/js/script.js  (обновлено — 2025-12-26)
// PURPOSE: модалка (open/close) + open по data-yy-modal-url (fetch, 404 ок) + close по фону/крестику/ESC.

// FILE: web/static/js/script.js  (обновлено — 2025-12-26)
// PURPOSE: минимальная модалка: open(arg) где arg = "url=..." или "text=..."; close по фону/крестику/ESC.

(function () {
  const $ = (s) => document.querySelector(s);

  function openModal(html) {
    const m = $("#yy-modal");
    if (!m) return;
    $("#yy-modal-body").innerHTML = html || "";
    m.classList.remove("hidden");
  }

  function closeModal() {
    const m = $("#yy-modal");
    if (!m) return;
    m.classList.add("hidden");
  }

  async function open(arg) {
    const s = String(arg || "");

    if (s.startsWith("url=")) {
      const url = s.slice(4).trim();
      let html = "";
      try {
        const r = await fetch(url, { credentials: "same-origin" });
        if (r && r.ok) html = await r.text();
      } catch (_) {}
      openModal(html);
      return;
    }

    if (s.startsWith("text=")) {
      openModal(s.slice(5));
      return;
    }

    openModal("");
  }

  document.addEventListener("click", (e) => {
    if (e.target.closest("[data-yy-modal-close]")) {
      e.preventDefault();
      closeModal();
      return;
    }

    const opener = e.target.closest("[data-yy-modal]");
    if (opener) {
      e.preventDefault();
      open(opener.getAttribute("data-yy-modal") || "");
    }
  });

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeModal();
  });

  window.YYModal = { open, close: closeModal };
})();
