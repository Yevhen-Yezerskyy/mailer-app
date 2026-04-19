// FILE: web/static/js/script.js
// DATE: 2026-01-17
// PURPOSE: Общий JS панели: sidebar, details icons, tabs, global loading, YYModal.
// CHANGE (fix modal):
// - close по клику на фон (backdrop) / контейнер модалки
// - остальное без изменений (url=/text=/post=...)

document.addEventListener("DOMContentLoaded", () => {

  /* ===============================
     SIDEBAR TOGGLE
     =============================== */
  const sidebar = document.getElementById("panel-sidebar");
  const btn = document.querySelector("[data-sidebar-toggle]");
  const expandedIcon = btn ? btn.querySelector("[data-sidebar-icon-expanded]") : null;
  const collapsedIcon = btn ? btn.querySelector("[data-sidebar-icon-collapsed]") : null;

  function syncSidebarToggleIcon() {
    if (!sidebar) return;
    const isCollapsed = sidebar.classList.contains("hidden");
    if (expandedIcon) expandedIcon.classList.toggle("hidden", isCollapsed);
    if (collapsedIcon) collapsedIcon.classList.toggle("hidden", !isCollapsed);
  }

  if (btn && sidebar) {
    syncSidebarToggleIcon();
    btn.addEventListener("click", () => {
      sidebar.classList.toggle("hidden");
      syncSidebarToggleIcon();
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

// FILE: web/static/js/script.js  (обновлено — 2026-01-17)
// PURPOSE: минимальная модалка: open(arg[, payload]) где arg = "url=..." | "text=..." | "post=..."; close по фону/крестику/ESC.

(function () {
  const $ = (s) => document.querySelector(s);
  const i18n = window.yyI18n || (document.documentElement && document.documentElement.yyI18n) || {};
  const t = (key, fallback) => {
    const v = i18n[key];
    return typeof v === "string" && v.trim() ? v : fallback;
  };

  function openModal(html) {
    const m = $("#yy-modal");
    const body = $("#yy-modal-body");
    if (!m || !body) return;
    body.innerHTML = html || "";
    m.classList.remove("hidden");
  }

  function closeModal() {
    const m = $("#yy-modal");
    const body = $("#yy-modal-body");
    if (!m) return;
    m.classList.add("hidden");
    if (body) body.innerHTML = "";
  }

  function _getCookie(name) {
    try {
      const m = document.cookie.match(new RegExp("(^|;\\s*)" + name + "=([^;]+)"));
      return m ? decodeURIComponent(m[2]) : "";
    } catch (_) {
      return "";
    }
  }

  async function open(arg, payload) {
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

    if (s.startsWith("post=")) {
      const url = s.slice(5).trim();
      let html = "";
      try {
        const csrftoken = _getCookie("csrftoken");
        const headers = { "Content-Type": "application/json" };
        if (csrftoken) headers["X-CSRFToken"] = csrftoken;

        const r = await fetch(url, {
          method: "POST",
          credentials: "same-origin",
          headers,
          body: JSON.stringify(payload || {}),
        });
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

  function _resolveFormActionUrl(form) {
    if (!form) return "";
    const raw = String(form.getAttribute("action") || "").trim();
    if (!raw) return "";
    try {
      return new URL(raw, window.location.href).toString();
    } catch (_) {
      return raw;
    }
  }

  function _setPauseInfoModalError(form, message) {
    const text = String(message || "").trim();
    const node = form ? form.querySelector("[data-pause-info-error='1']") : null;
    const wrap = form ? form.querySelector("[data-pause-info-error-wrap='1']") : null;
    if (node) node.textContent = text;
    if (wrap) wrap.classList.toggle("hidden", !text);
  }

  function _handlePauseInfoContinueSubmit(event) {
    const form = event.target.closest("[data-pause-info-continue-form='1']");
    if (!form) return;

    event.preventDefault();
    _setPauseInfoModalError(form, "");

    const data = new FormData(form);
    const actionUrl = _resolveFormActionUrl(form) || window.location.href;
    const csrfInput = form.querySelector("input[name='csrfmiddlewaretoken']");
    const csrfFromInput = csrfInput ? String(csrfInput.value || "").trim() : "";
    const csrfFromCookie = String(_getCookie("csrftoken") || "").trim();
    const csrfToken = csrfFromInput || csrfFromCookie;
    if (!String(data.get("csrfmiddlewaretoken") || "").trim() && csrfToken) {
      data.set("csrfmiddlewaretoken", csrfToken);
    }

    if (window.YYWaitModal && typeof window.YYWaitModal.open === "function") {
      window.YYWaitModal.open();
    }

    window.fetch(actionUrl, {
      method: "POST",
      body: data,
      credentials: "same-origin",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
        "X-CSRFToken": csrfToken,
      },
    }).then(function (response) {
      return response.text().then(function (text) {
        let payload = {};
        try {
          payload = JSON.parse(String(text || ""));
        } catch (_) {
          payload = {};
        }
        return { response: response, payload: payload, text: String(text || "") };
      });
    }).then(function (result) {
      const payload = result.payload || {};
      if (result.response.ok) {
        if (payload.ok === false) {
          _setPauseInfoModalError(form, payload.error || t("request_failed", "Request failed"));
          return;
        }
        window.location.reload();
        return;
      }

      const bodyText = String(result.text || "");
      if (payload && payload.error) {
        _setPauseInfoModalError(form, payload.error);
        return;
      }
      if (bodyText && /csrf/i.test(bodyText)) {
        _setPauseInfoModalError(form, t("csrf_validation_failed", "CSRF validation failed"));
        return;
      }
      _setPauseInfoModalError(form, t("request_failed", "Request failed"));
    }).catch(function () {
      _setPauseInfoModalError(form, t("request_failed", "Request failed"));
    }).finally(function () {
      if (window.YYWaitModal && typeof window.YYWaitModal.close === "function") {
        window.YYWaitModal.close();
      }
    });
  }

  if (!window.__YY_PAUSE_INFO_HANDLER_BOUND__) {
    document.addEventListener("submit", _handlePauseInfoContinueSubmit);
    window.__YY_PAUSE_INFO_HANDLER_BOUND__ = true;
  }

  document.addEventListener("click", (e) => {
    // close by X / any element with data-yy-modal-close
    if (e.target.closest("[data-yy-modal-close]")) {
      e.preventDefault();
      closeModal();
      return;
    }

    // FIX: close by backdrop click (любая из типовых структур)
    const modalRoot = $("#yy-modal");
    if (modalRoot && !modalRoot.classList.contains("hidden")) {
      const isRootClick = (e.target === modalRoot);
      const isBackdropClick =
        !!e.target.closest("#yy-modal-backdrop") ||
        !!e.target.closest("[data-yy-modal-backdrop]") ||
        (e.target.id === "yy-modal-backdrop");

      if (isRootClick || isBackdropClick) {
        e.preventDefault();
        closeModal();
        return;
      }
    }

    // open
    const opener = e.target.closest("[data-yy-modal]");
    if (opener) {
      e.preventDefault();
      const arg = opener.getAttribute("data-yy-modal") || "";
      let payload = null;

      const raw = opener.getAttribute("data-yy-modal-payload") || "";
      if (raw) {
        try {
          payload = JSON.parse(raw);
        } catch (_) {
          payload = null;
        }
      }

      open(arg, payload);
    }
  });

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeModal();
  });

  window.YYModal = { open, close: closeModal };
})();
