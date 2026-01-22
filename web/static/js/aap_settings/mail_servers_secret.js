// FILE: web/static/js/aap_settings/mail_servers_secret.js
// DATE: 2026-01-22
// PURPOSE: Settings → Mail servers: secret reveal (modal + fetch) + dirty-watch формы.
// CHANGE:
// - Перенесён inline JS из mail_servers.html.
// - Dirty-watch: если форма изменена — показываем yyMailChecksDirtyHint и скрываем yyMailChecksPanel.

(function () {
  const SECRET_MASK = "********";

  let yySecretModalState = null;

  function yySecretModalOpen(state) {
    yySecretModalState = state;
    const el = document.getElementById("yy-secret-modal");
    if (el) el.classList.remove("hidden");
  }

  function yySecretModalClose() {
    yySecretModalState = null;
    const el = document.getElementById("yy-secret-modal");
    if (el) el.classList.add("hidden");
  }

  async function yySecretModalConfirm() {
    const st = yySecretModalState;
    yySecretModalClose();
    if (!st) return;
    await yyRevealSecret(st.kind, st.inputId);
  }

  function yyGetMailboxToken() {
    const hidden = document.querySelector('input[name="id"]');
    if (!hidden) return "";
    return (hidden.value || "").trim();
  }

  async function yyRevealSecret(kind, inputId) {
    const token = yyGetMailboxToken();
    if (!token) return;

    const url = `secret/?id=${encodeURIComponent(token)}&kind=${encodeURIComponent(kind)}`;

    let data = null;
    try {
      const resp = await fetch(url, { method: "GET", credentials: "same-origin" });
      data = await resp.json();
    } catch (e) {
      alert("Error");
      return;
    }

    if (!data || !data.ok) {
      alert("Error");
      return;
    }

    const el = document.getElementById(inputId);
    if (!el) return;

    el.value = data.secret || "";
    el.removeAttribute("readonly");
    el.removeAttribute("data-yy-masked");
    el.type = "text";
    el.focus();
  }

  function yyTogglePasswordOrReveal(kind, inputId) {
    const el = document.getElementById(inputId);
    if (!el) return;

    const masked = (el.getAttribute("data-yy-masked") || "") === "1";
    if (masked) {
      yySecretModalOpen({ kind: kind, inputId: inputId });
      return;
    }

    el.type = (el.type === "password") ? "text" : "password";
  }

  function yyDirtyToggle(isDirty) {
    const hint = document.getElementById("yyMailChecksDirtyHint");
    const panel = document.getElementById("yyMailChecksPanel");
    if (!hint || !panel) return;

    if (isDirty) {
      hint.classList.remove("hidden");
      panel.classList.add("hidden");
    } else {
      hint.classList.add("hidden");
      panel.classList.remove("hidden");
    }
  }

  function yyFormSnapshot(form) {
    const items = [];

    const els = Array.from(form.elements || []);
    for (const el of els) {
      if (!el || el.disabled) continue;

      const name = (el.name || "").trim();
      if (!name) continue;

      if (name === "csrfmiddlewaretoken") continue;
      if (name === "action") continue;

      const tag = (el.tagName || "").toLowerCase();
      const type = (el.type || "").toLowerCase();

      if (type === "submit" || type === "button") continue;

      if (type === "checkbox" || type === "radio") {
        items.push([name, el.checked ? "1" : "0"]);
        continue;
      }

      let v = (el.value || "");

      // masked secrets: "********" считаем как "не меняли"
      if (type === "password" || name.endsWith("_secret")) {
        if ((el.getAttribute("data-yy-masked") || "") === "1" && v === SECRET_MASK) {
          v = "";
        }
      }

      if (tag === "select" && el.multiple) {
        const vals = Array.from(el.selectedOptions || []).map((o) => (o.value || "")).sort();
        v = vals.join("|");
      }

      items.push([name, String(v)]);
    }

    items.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : (a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0)));
    return items.map(([k, v]) => `${k}=${v}`).join("&");
  }

  function yyInitDirtyWatch() {
    const form = document.getElementById("yyMailServerForm");
    if (!form) return;

    const initial = yyFormSnapshot(form);
    let lastDirty = false;

    function recompute() {
      const now = yyFormSnapshot(form);
      const dirty = now !== initial;
      if (dirty === lastDirty) return;
      lastDirty = dirty;
      yyDirtyToggle(dirty);
    }

    // стартовое состояние
    yyDirtyToggle(false);

    form.addEventListener("input", recompute, true);
    form.addEventListener("change", recompute, true);

    // если что-то прогрузилось поздно (autofill)
    setTimeout(recompute, 0);
    setTimeout(recompute, 250);
  }

  document.addEventListener("DOMContentLoaded", function () {
    yyInitDirtyWatch();
  });

  // export for onclick in HTML
  window.yySecretModalOpen = yySecretModalOpen;
  window.yySecretModalClose = yySecretModalClose;
  window.yySecretModalConfirm = yySecretModalConfirm;
  window.yyTogglePasswordOrReveal = yyTogglePasswordOrReveal;
})();
