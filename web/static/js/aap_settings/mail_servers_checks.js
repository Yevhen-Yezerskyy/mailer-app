// FILE: web/static/js/aap_settings/mail_servers_checks.js
// DATE: 2026-01-25
// PURPOSE: Settings → Mail servers: теперь только DOMAIN check через API.
// CHANGE: ожидает JSON {ok, action, tech:{...}, reputation:{...}} и пишет его в textarea.

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
    return { ok: r.ok, text: await r.text() };
  }

  window.yyMailServersCheck = async function (action, btnId) {
    // action expected: "check_domain"
    const form = byId("yySmtpForm") || document.querySelector("form[data-api-url]");
    const out = byId("yyMailChecksOut");
    const btn = byId(btnId);

    if (!form) return;
    const url = form.getAttribute("data-api-url") || "";
    if (!url) return;

    const mbIdEl = form.querySelector('input[name="id"]');
    const mailbox_ui_id = mbIdEl ? (mbIdEl.value || "") : "";

    if (out) out.value = "…";

    setBtnLoading(btn, true);
    try {
      const res = await postJson(url, { action: action, id: mailbox_ui_id });

      if (out) {
        if (res && typeof res === "object") {
          out.value = JSON.stringify(res, null, 2);
        } else {
          out.value = String(res || "");
        }
      }
    } catch (e) {
      if (out) out.value = "ERROR: " + (e && e.message ? e.message : String(e));
    } finally {
      setBtnLoading(btn, false);
    }
  };
})();
