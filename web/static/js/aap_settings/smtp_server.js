// FILE: web/static/js/aap_settings/smtp_server.js
// DATE: 2026-01-24
// PURPOSE: Settings → SMTP server: переключение LOGIN/Google/Microsoft, пресеты (overwrite host/port/security), скрытие Save в OAuth.
// CHANGE: удалено автозаполнение логина из email/пресета (логин задаётся сервер-сайд).

(function () {
  function byId(id) { return document.getElementById(id); }

  function setAuth(v) {
    const auth = byId("yyAuthType");
    if (auth) auth.value = v;

    const login = byId("yyLoginBlock");
    const oauth = byId("yyOauthBlock");
    const save  = byId("yySaveWrap");

    const btnL = byId("yyAuthBtnLogin");
    const btnG = byId("yyAuthBtnGoogle");
    const btnM = byId("yyAuthBtnMicrosoft");

    const on = (btn, yes) => {
      if (!btn) return;
      btn.classList.toggle("YY-BUTTON_MAIN", yes);
      btn.classList.toggle("YY-BUTTON_TAB_MAIN", !yes);
    };

    if (v === "login") {
      if (login) login.classList.remove("hidden");
      if (oauth) oauth.classList.add("hidden");
      if (save)  save.classList.remove("hidden");
      on(btnL, true); on(btnG, false); on(btnM, false);
      return;
    }

    if (login) login.classList.add("hidden");
    if (oauth) oauth.classList.remove("hidden");
    if (save)  save.classList.add("hidden");

    on(btnL, false);
    on(btnG, v === "google_oauth2");
    on(btnM, v === "microsoft_oauth2");
  }

  function getPresets() {
    const el = byId("yyPresets");
    if (!el) return {};
    try { return JSON.parse(el.textContent || "{}"); } catch (e) { return {}; }
  }

  function applyPreset(code) {
    if (!code) return;
    const p = getPresets()[code];
    if (!p) return;

    const host = byId("yyHost");
    const port = byId("yyPort");
    const sec  = byId("yySecurity");

    if (host) host.value = p.host || "";
    if (port) port.value = p.port != null ? String(p.port) : "";
    if (sec)  sec.value  = p.security || (sec.value || "starttls");
  }

  window.addEventListener("DOMContentLoaded", () => {
    const auth = byId("yyAuthType");
    setAuth(auth ? (auth.value || "login") : "login");

    const btnL = byId("yyAuthBtnLogin");
    const btnG = byId("yyAuthBtnGoogle");
    const btnM = byId("yyAuthBtnMicrosoft");

    if (btnL) btnL.addEventListener("click", () => setAuth("login"));
    if (btnG) btnG.addEventListener("click", () => setAuth("google_oauth2"));
    if (btnM) btnM.addEventListener("click", () => setAuth("microsoft_oauth2"));

    const sel = byId("yyPresetSelect");
    if (sel) sel.addEventListener("change", () => applyPreset((sel.value || "").trim()));
  });
})();
