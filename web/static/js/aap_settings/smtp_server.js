// FILE: web/static/js/aap_settings/smtp_server.js
// DATE: 2026-01-26
// PURPOSE: SMTP server page: auth_type tabs + apply preset to LOGIN fields via [name="..."].
// CHANGE:
// - Active tab highlight via inline styles (bg/text/border) because YY-* class expansion happens server-side.
// - OAuth stubs are separate top-level blocks: yyOauthGoogleBlock / yyOauthMicrosoftBlock (no yyOauthBlock wrapper).

(function () {
  function byId(id) { return document.getElementById(id); }
  function qByName(name) { return document.querySelector('[name="' + name + '"]'); }

  const ACTIVE = {
    bg: "#d6fdda",
    text: "#016b09",
    border: "#71d0f4",
  };

  function setBtnActiveStyle(btn, active) {
    if (!btn) return;

    if (active) {
      btn.style.backgroundColor = ACTIVE.bg;
      btn.style.color = ACTIVE.text;
      btn.style.borderColor = ACTIVE.border;
    } else {
      btn.style.backgroundColor = "";
      btn.style.color = "";
      btn.style.borderColor = "";
    }
  }

  function show(el, yes) {
    if (!el) return;
    el.classList.toggle("hidden", !yes);
  }

  function setAuth(v) {
    const auth = qByName("auth_type");
    if (auth) auth.value = v;

    const login = byId("yyLoginBlock");
    const save  = byId("yySaveWrap");

    const oauthG = byId("yyOauthGoogleBlock");
    const oauthM = byId("yyOauthMicrosoftBlock");

    const btnL = byId("yyAuthBtnLogin");
    const btnG = byId("yyAuthBtnGoogle");
    const btnM = byId("yyAuthBtnMicrosoft");

    if (v === "LOGIN") {
      show(login, true);
      show(save, true);
      show(oauthG, false);
      show(oauthM, false);

      setBtnActiveStyle(btnL, true);
      setBtnActiveStyle(btnG, false);
      setBtnActiveStyle(btnM, false);
      return;
    }

    show(login, false);
    show(save, false);

    const isG = (v === "GOOGLE_OAUTH_2_0");
    const isM = (v === "MICROSOFT_OAUTH_2_0");

    show(oauthG, isG);
    show(oauthM, isM);

    setBtnActiveStyle(btnL, false);
    setBtnActiveStyle(btnG, isG);
    setBtnActiveStyle(btnM, isM);
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
    if (sec && p.security != null) sec.value = String(p.security);
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
