// FILE: web/static/js/aap_settings/mail_servers_presets.js
// DATE: 2026-01-23
// PURPOSE: Settings → Mail servers (SMTP/IMAP): авто-применение пресета без кнопки.
// CHANGE: onChange select -> подставляет host/port/security/auth_type.

(function () {
  function byId(id) { return document.getElementById(id); }

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

    const host = byId("yyHost");
    const port = byId("yyPort");
    const sec  = byId("yySecurity");
    const auth = byId("yyAuthType");

    if (host && !host.value) host.value = p.host || "";
    if (port && !port.value) {
      const ports = Array.isArray(p.ports) ? p.ports : [];
      if (ports.length) port.value = String(ports[0]);
    }
    if (sec && (!sec.value || sec.value === "none")) sec.value = p.security || sec.value;
    if (auth && (!auth.value || auth.value === "login")) auth.value = p.auth_type || auth.value;

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
