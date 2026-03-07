// FILE: web/static/js/aap_settings/mail_servers_presets.js
// DATE: 2026-01-23
// PURPOSE: Settings → Mail servers (SMTP/IMAP): авто-применение пресета без кнопки.
// CHANGE: onChange select -> подставляет host/port/security/auth_type.

(function () {
  function byId(id) { return document.getElementById(id); }
  function byName(name) { return document.querySelector('[name="' + name + '"]'); }

  function getPresets() {
    const el = byId("yyPresets");
    if (!el) return {};
    try { return JSON.parse(el.textContent || "{}"); } catch (e) { return {}; }
  }

  function applyPreset(code) {
    if (!code) return;
    const presets = getPresets();
    const p = presets[code];
    if (!p) return;

    const host = byName("host");
    const port = byName("port");
    const sec  = byName("security");
    const auth = byName("auth_type");

    if (host) host.value = p.host || "";
    if (port) port.value = p.port != null ? String(p.port) : "";
    if (sec) sec.value = p.security || "";
    if (auth) auth.value = p.auth_type || auth.value;

    // триггерим change, чтобы auth-js мог скрыть/показать блоки
    if (auth) {
      const ev = new Event("change", { bubbles: true });
      auth.dispatchEvent(ev);
    }
  }

  window.addEventListener("DOMContentLoaded", () => {
    const sel = byId("yyPresetSelect");
    if (!sel) return;
    sel.addEventListener("change", () => {
      applyPreset((sel.value || "").trim());
    });
  });
})();
