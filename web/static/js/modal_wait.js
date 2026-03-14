// FILE: web/static/js/modal_wait.js
// DATE: 2026-03-14
// PURPOSE: Global non-closable wait overlay with the same backdrop style as modal.

(function () {
  function ensureRoot() {
    let el = document.getElementById("yy-wait-loading");
    if (el) return el;

    el = document.createElement("div");
    el.id = "yy-wait-loading";
    el.className = "yy-loading hidden";
    el.setAttribute("aria-hidden", "true");
    el.innerHTML =
      '<div class="yy-loading__backdrop" style="background: rgba(5, 112, 235, 0.012);"></div>' +
      '<div class="yy-loading__box" role="status" aria-live="polite" aria-label="Loading">' +
        '<img src="/static/img/spinner.svg" alt="" class="w-[150px] h-[150px]">' +
      '</div>';
    document.body.appendChild(el);
    return el;
  }

  function open() {
    const el = ensureRoot();
    el.classList.remove("hidden");
    el.setAttribute("aria-hidden", "false");
  }

  function close() {
    const el = ensureRoot();
    if (!el) return;
    el.classList.add("hidden");
    el.setAttribute("aria-hidden", "true");
  }

  window.YYWaitModal = { open, close };
})();
