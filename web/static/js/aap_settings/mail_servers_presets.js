// FILE: web/static/js/aap_settings/mail_servers_presets.js
// DATE: 2026-04-14
// PURPOSE: IMAP page: auth switch buttons + preset apply.

(function () {
  function byId(id) { return document.getElementById(id); }
  function byName(name) { return document.querySelector('[name="' + name + '"]'); }
  function show(el, yes) {
    if (!el) return;
    el.classList.toggle("hidden", !yes);
  }

  function mappedButtonClass(key, extra) {
    const classMap = window.yyClassMap || (document.documentElement && document.documentElement.yyClassMap) || {};
    const mapped = String(classMap[key] || "").trim();
    const tail = String(extra || "").trim();
    if (mapped) return [key, mapped, tail].filter(Boolean).join(" ");
    return [key, tail].filter(Boolean).join(" ");
  }

  function setBtnStateClass(btn, active) {
    if (!btn) return;
    const extra = btn.getAttribute("data-base-extra") || "";
    const key = active ? "YY-BUTTON_GREEN" : "YY-BUTTON_MAIN";
    btn.setAttribute("class", mappedButtonClass(key, extra));
  }

  function setAuth(v) {
    const auth = byName("auth_type");
    if (auth) auth.value = v;

    const btnL = byId("yyAuthBtnLogin");
    const btnG = byId("yyAuthBtnGoogle");
    const btnM = byId("yyAuthBtnMicrosoft");

    const loginBlock = byId("yyLoginBlock");
    const oauthG = byId("yyOauthGoogleBlock");
    const oauthM = byId("yyOauthMicrosoftBlock");

    const isLogin = (v === "LOGIN");
    const isG = (v === "GOOGLE_OAUTH_2_0");
    const isM = (v === "MICROSOFT_OAUTH_2_0");

    setBtnStateClass(btnL, isLogin);
    setBtnStateClass(btnG, isG);
    setBtnStateClass(btnM, isM);

    show(loginBlock, isLogin);
    show(oauthG, isG);
    show(oauthM, isM);
  }

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

    if (auth) setAuth(auth.value || "LOGIN");
  }

  window.addEventListener("DOMContentLoaded", () => {
    const auth = byName("auth_type");
    setAuth(auth ? (auth.value || "LOGIN") : "LOGIN");

    const btnL = byId("yyAuthBtnLogin");
    const btnG = byId("yyAuthBtnGoogle");
    const btnM = byId("yyAuthBtnMicrosoft");
    if (btnL) btnL.addEventListener("click", () => setAuth("LOGIN"));
    if (btnG) btnG.addEventListener("click", () => setAuth("GOOGLE_OAUTH_2_0"));
    if (btnM) btnM.addEventListener("click", () => setAuth("MICROSOFT_OAUTH_2_0"));

    if (auth) {
      auth.addEventListener("change", () => {
        setAuth(auth.value || "LOGIN");
      });
    }

    const sel = byId("yyPresetSelect");
    if (!sel) return;
    sel.addEventListener("change", () => {
      applyPreset((sel.value || "").trim());
    });
  });
})();
