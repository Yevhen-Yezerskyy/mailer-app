// FILE: web/static/js/aap_settings/smtp_server.js
// DATE: 2026-01-24
// PURPOSE: SMTP server page: auth_type tabs + apply preset to LOGIN fields via [name="..."].
// CHANGE: canonical auth_type values; zero dependency on Django-generated ids for inputs.

(function () {
  function byId(id) { return document.getElementById(id); }
  function qByName(name) { return document.querySelector('[name="' + name + '"]'); }

  function setAuth(v) {
    const auth = qByName("auth_type");
    if (auth) auth.value = v;

    const login = byId("yyLoginBlock");
    const oauth = byId("yyOauthBlock");
    const save  = byId("yySaveWrap");

    const btnL = byId("yyAuthBtnLogin");
    const btnG = byId("yyAuthBtnGoogle");
    const btnM = byId("yyAuthBtnMicrosoft");

    function mark(btn, active) {
      if (!btn) return;
      btn.classList.toggle("YY-BUTTON_MAIN", active);
      btn.classList.toggle("YY-BUTTON_TAB_MAIN", !active);
    }

    if (v === "LOGIN") {
      if (login) login.classList.remove("hidden");
      if (oauth) oauth.classList.add("hidden");
      if (save)  save.classList.remove("hidden");
      mark(btnL, true); mark(btnG, false); mark(btnM, false);
      return;
    }

    if (login) login.classList.add("hidden");
    if (oauth) oauth.classList.remove("hidden");
    if (save)  save.classList.add("hidden");

    mark(btnL, false);
    mark(btnG, v === "GOOGLE_OAUTH_2_0");
    mark(btnM, v === "MICROSOFT_OAUTH_2_0");
  }

  function getPresets() {
    const el = byId("yyPresets");
    if (!el) return {};
    try { return JSON.parse(el.textContent || "{}"); } catch (e) { return {}; }
  }

  function applyPreset(presetId) {
    if (!presetId) return;
    const p = getPresets()[presetId];
    if (!p) return;

    const host = qByName("host");
    const port = qByName("port");
    const sec  = qByName("security");

    if (host && p.host != null) host.value = String(p.host);
    if (port && p.port != null) port.value = String(p.port);
    if (sec  && p.security != null) sec.value = String(p.security);
  }

  window.addEventListener("DOMContentLoaded", () => {
    const auth = qByName("auth_type");
    setAuth(auth ? (auth.value || "LOGIN") : "LOGIN");

    const btnL = byId("yyAuthBtnLogin");
    const btnG = byId("yyAuthBtnGoogle");
    const btnM = byId("yyAuthBtnMicrosoft");

    if (btnL) btnL.addEventListener("click", () => setAuth("LOGIN"));
    if (btnG) btnG.addEventListener("click", () => setAuth("GOOGLE_OAUTH_2_0"));
    if (btnM) btnM.addEventListener("click", () => setAuth("MICROSOFT_OAUTH_2_0"));

    const sel = byId("yyPresetSelect");
    if (sel) sel.addEventListener("change", () => applyPreset((sel.value || "").trim()));
  });
})();
