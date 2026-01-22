// FILE: web/static/js/aap_settings/mail_servers_checks.js
// DATE: 2026-01-22
// PURPOSE: Settings → Mail servers: "Проверки".
// CHANGE: Send mailbox id(ui) with request; show status/message + pretty JSON.

(function () {
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
    yyBtnSpin(btn, true);

    let data = null;
    try {
      data = await yyPostCheck(action);
    } catch (e) {
      data = { ok: false, error: "network" };
    }

    yyBtnSpin(btn, false);

    if (!data || !data.ok) {
      yySetOut("ERROR: " + (data && (data.error || data.message) ? (data.error || data.message) : "unknown"));
      return;
    }

    const head =
      (data.status ? data.status + ": " : "") +
      (data.message || "OK") +
      (data.action ? " [" + data.action + "]" : "");
    const tail = data.data ? "\n\n" + JSON.stringify(data.data, null, 2) : "";
    yySetOut(head + tail);
  };
})();
