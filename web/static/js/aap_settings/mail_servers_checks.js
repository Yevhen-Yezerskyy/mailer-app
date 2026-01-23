// FILE: web/static/js/aap_settings/mail_servers_checks.js
// DATE: 2026-01-23
// PURPOSE: Settings → Mail servers: "Проверки" (SMTP/IMAP/DOMAIN) через textarea.
// CHANGE: Кнопка после клика блокируется минимум на 5 секунд (крутилка), чтобы не спамить провайдера.

(function () {
  const HOLD_MS = 5000;

  function yyCsrftoken() {
    const m = document.cookie.match(/(?:^|;\s*)csrftoken=([^;]+)/);
    return m ? decodeURIComponent(m[1]) : "";
  }

  function yyBtnSpin(btn, on) {
    if (!btn) return;
    if (on) {
      btn.dataset.yyText = btn.textContent || "";
      btn.disabled = true;
      btn.innerHTML =
        '<span class="inline-flex items-center gap-2">' +
        '<span class="inline-block animate-spin">⏳</span>' +
        "<span>...</span>" +
        "</span>";
    } else {
      btn.disabled = false;
      btn.textContent = btn.dataset.yyText || btn.textContent || "";
      delete btn.dataset.yyText;
    }
  }

  function yySetOut(text) {
    const ta = document.getElementById("yyMailChecksOut");
    if (!ta) return;
    ta.value = text || "";
  }

  function yyGetMailboxToken() {
    const form = document.getElementById("yyMailServerForm");
    if (!form) return "";
    const el = form.querySelector('input[name="id"]');
    return el ? (el.value || "").trim() : "";
  }

  async function yyPostCheck(action) {
    const fd = new FormData();
    fd.append("action", action);
    const tok = yyGetMailboxToken();
    if (tok) fd.append("id", tok);

    const resp = await fetch("api/", {
      method: "POST",
      body: fd,
      credentials: "same-origin",
      headers: { "X-CSRFToken": yyCsrftoken() },
    });

    return await resp.json();
  }

  window.yyMailServersCheck = async function yyMailServersCheck(action, btnId) {
    const btn = document.getElementById(btnId);
    if (!btn || btn.disabled) return;

    const tStart = Date.now();
    yyBtnSpin(btn, true);

    let data = null;
    try {
      data = await yyPostCheck(action);
    } catch (e) {
      data = { ok: false, error: "network" };
    }

    const elapsed = Date.now() - tStart;
    const wait = Math.max(0, HOLD_MS - elapsed);

    window.setTimeout(() => {
      yyBtnSpin(btn, false);

      if (!data || !data.ok) {
        yySetOut("ERROR: " + (data && (data.error || data.message) ? (data.error || data.message) : "unknown"));
        return;
      }

      // server already builds human message for textarea
      yySetOut(data.message || "OK");
    }, wait);
  };
})();
