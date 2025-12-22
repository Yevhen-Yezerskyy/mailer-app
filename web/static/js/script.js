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

/* ===============================
   BUTTON LOADING OVERLAY
   =============================== */

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
