// FILE: web/static/js/aap_settings/mail_servers_checks.js
// DATE: 2026-01-26
// PURPOSE: Settings → Mail servers: checks via API (domain / SMTP / IMAP) + test mail.
// CHANGE:
// - After each check: stash output to sessionStorage, reload page (statuses update from view), restore output back into textarea.
// - In SMTP/IMAP edit state: when form becomes dirty, disable check buttons (prevents checking unsaved config).
// - Keep backward compatibility with existing buttons/ids.

(function () {
  function byId(id) { return document.getElementById(id); }

  // -------------------------
  // Persist output across reload (to update status blocks)
  // -------------------------
  const OUT_KEY = "yyMailServersChecksOut:v1:" + (location && location.pathname ? location.pathname : "");

  function _readOutMap() {
    try {
      const raw = sessionStorage.getItem(OUT_KEY) || "";
      const obj = raw ? JSON.parse(raw) : {};
      return obj && typeof obj === "object" ? obj : {};
    } catch (e) {
      return {};
    }
  }

  function stashOut(outId, value) {
    if (!outId) return;
    const m = _readOutMap();
    m[String(outId)] = String(value || "");
    try { sessionStorage.setItem(OUT_KEY, JSON.stringify(m)); } catch (e) {}
  }

  function restoreOut() {
    const m = _readOutMap();
    const keys = Object.keys(m || {});
    if (!keys.length) return;

    for (const k of keys) {
      const el = byId(k);
      if (!el) continue;
      try { el.value = m[k]; } catch (e) {}
    }

    try { sessionStorage.removeItem(OUT_KEY); } catch (e) {}
  }

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

  function scheduleReload() {
    try { window.location.reload(); } catch (e) {}
  }

  // -------------------------
  // CHECK (domain / smtp / imap) — unified
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
      if (out && out.id) stashOut(out.id, out.value);
    } catch (e) {
      if (out) out.value = "ERROR: " + (e.message || e);
      if (out && out.id) stashOut(out.id, out.value);
    } finally {
      setBtnLoading(btn, false);
      scheduleReload();
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
      if (out && out.id) stashOut(out.id, out.value);
    } catch (e) {
      if (out) out.value = "ERROR: " + (e.message || e);
      if (out && out.id) stashOut(out.id, out.value);
    } finally {
      setBtnLoading(btn, false);
      scheduleReload();
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

  // -------------------------
  // Init: restore output from sessionStorage after reload
  // -------------------------
  (function initRestore() {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", restoreOut);
    } else {
      restoreOut();
    }
  })();

  // -------------------------
  // Init: disable check buttons when form is dirty (edit only)
  // -------------------------
  (function initDirtyGuard() {
    const form = byId("yySmtpForm");
    if (!form) return;
    const state = (form.getAttribute("data-state") || "").toLowerCase();
    if (state !== "edit") return;

    const btnIds = ["yyCheckSmtpBtn", "yySendTestMailBtn", "yyCheckImapBtn"];
    const btns = btnIds.map(byId).filter(Boolean);
    if (!btns.length) return;

    const fields = Array.from(form.querySelectorAll("input, select, textarea")).filter((el) => {
      if (!el) return false;
      const tag = (el.tagName || "").toLowerCase();
      if (tag === "button") return false;
      const t = (el.getAttribute("type") || "").toLowerCase();
      if (t === "hidden" || t === "submit" || t === "button") return false;
      const name = (el.getAttribute("name") || "").toLowerCase();
      if (name === "csrfmiddlewaretoken") return false;
      return true;
    });

    function keyOf(el) {
      return el.getAttribute("name") || el.id || "";
    }

    function valOf(el) {
      const t = (el.getAttribute("type") || "").toLowerCase();
      if (t === "checkbox" || t === "radio") return el.checked ? "1" : "0";
      return (el.value || "");
    }

    const snap = {};
    for (const el of fields) {
      const k = keyOf(el);
      if (!k) continue;
      snap[k] = valOf(el);
    }

    function isDirty() {
      for (const el of fields) {
        const k = keyOf(el);
        if (!k) continue;
        if ((snap[k] ?? "") !== valOf(el)) return true;
      }
      return false;
    }

    function syncDirty() {
      const dirty = isDirty();
      for (const b of btns) setBtnDisabled(b, dirty);
    }

    syncDirty();
    for (const el of fields) {
      el.addEventListener("input", syncDirty);
      el.addEventListener("change", syncDirty);
    }
  })();

})();
