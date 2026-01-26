// FILE: web/static/js/aap_settings/mail_servers_checks.js
// DATE: 2026-01-26
// PURPOSE: Settings → Mail servers: SMTP auth check + send test mail.
// CHANGE:
// - Disable "send test mail" button when email is empty
// - Backward compatible with existing checks

(function () {
  function byId(id) { return document.getElementById(id); }

  function getCsrfToken() {
    const el = document.querySelector('input[name="csrfmiddlewaretoken"]');
    return el ? el.value : "";
  }

  function setBtnLoading(btn, on) {
    if (!btn) return;
    btn.disabled = !!on;
    btn.classList.toggle("opacity-60", !!on);
    btn.classList.toggle("cursor-not-allowed", !!on);
  }

  function setBtnDisabled(btn, on) {
    if (!btn) return;
    btn.disabled = !!on;
    btn.classList.toggle("opacity-60", !!on);
    btn.classList.toggle("cursor-not-allowed", !!on);
  }

  async function postJson(url, payload) {
    const r = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRFToken": getCsrfToken(),
      },
      body: JSON.stringify(payload),
    });

    const ct = (r.headers.get("content-type") || "").toLowerCase();
    if (ct.includes("application/json")) return await r.json();
    return { text: await r.text() };
  }

  function renderOut(out, res) {
    if (!out) return;
    if (res && typeof res === "object") {
      let s = JSON.stringify(res, null, 2);
      s = s.replace(/^\{\s*\n?/, "").replace(/\n?\s*\}$/, "");
      out.value = s.trim();
    } else {
      out.value = String(res || "");
    }
  }

  // -------------------------
  // SMTP AUTH CHECK
  // -------------------------
  window.yyMailServersCheck = async function (action, btnId, outId) {
    const form = byId("yySmtpForm");
    if (!form) return;

    const btn = byId(btnId);
    const out =
      byId(outId) ||
      (btn && byId(btn.getAttribute("data-output-id"))) ||
      byId("yyMailChecksOut");

    const url = form.getAttribute("data-api-url");
    const mbId = form.querySelector('input[name="id"]')?.value || "";

    if (out) out.value = "…";
    setBtnLoading(btn, true);

    try {
      const res = await postJson(url, { action: action, id: mbId });
      renderOut(out, res);
    } catch (e) {
      if (out) out.value = "ERROR: " + (e.message || e);
    } finally {
      setBtnLoading(btn, false);
    }
  };

  // -------------------------
  // SEND TEST MAIL
  // -------------------------
  window.yySendTestMail = async function (btnId) {
    const form = byId("yySmtpForm");
    if (!form) return;

    const toEl = byId("yyTestMailTo");
    const to = toEl?.value || "";
    if (!to) return;

    const btn = byId(btnId);
    const out = byId(btn?.getAttribute("data-output-id") || "");

    const url = form.getAttribute("data-api-url");
    const mbId = form.querySelector('input[name="id"]')?.value || "";

    if (out) out.value = "…";
    setBtnLoading(btn, true);

    try {
      const res = await postJson(url, {
        action: "send_test_mail",
        id: mbId,
        to: to,
      });
      renderOut(out, res);
    } catch (e) {
      if (out) out.value = "ERROR: " + (e.message || e);
    } finally {
      setBtnLoading(btn, false);
    }
  };

  // -------------------------
  // Init: disable send button when email empty
  // -------------------------
  (function initSendMailGuard() {
    const toEl = byId("yyTestMailTo");
    const btn = byId("yySendTestMailBtn");
    if (!toEl || !btn) return;

    const sync = () => setBtnDisabled(btn, !toEl.value.trim());
    sync();
    toEl.addEventListener("input", sync);
  })();

})();
