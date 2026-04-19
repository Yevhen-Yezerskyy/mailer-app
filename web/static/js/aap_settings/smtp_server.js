// FILE: web/static/js/aap_settings/smtp_server.js
// DATE: 2026-02-25
// PURPOSE: SMTP server page: auth_type tabs + presets for LOGIN and RELAY_NOAUTH.

(function () {
  function byId(id) { return document.getElementById(id); }
  function qByName(name) { return document.querySelector('[name="' + name + '"]'); }
  const i18n = window.yyI18n || (document.documentElement && document.documentElement.yyI18n) || {};
  function t(key, fallback) {
    const v = i18n[key];
    return typeof v === "string" && v.trim() ? v : fallback;
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

  function show(el, yes) {
    if (!el) return;
    el.classList.toggle("hidden", !yes);
  }

  function normSecurity(v) {
    const s = String(v || "").trim().toLowerCase();
    if (s === "tls") return "ssl";
    return s;
  }

  function parseJsonEl(id) {
    const el = byId(id);
    if (!el) return {};
    try {
      const obj = JSON.parse(el.textContent || "{}");
      return obj && typeof obj === "object" ? obj : {};
    } catch (e) {
      return {};
    }
  }

  function activePresetMap(authType) {
    if (authType === "RELAY_NOAUTH") return parseJsonEl("yyRelayPresets");
    return parseJsonEl("yyLoginPresets");
  }

  function refillPresetOptions(authType) {
    const sel = byId("yyPresetSelect");
    if (!sel) return;

    const oldValue = (sel.value || "").trim();
    const map = activePresetMap(authType);
    const keys = Object.keys(map || {});

    sel.innerHTML = "";
    const ph = document.createElement("option");
    ph.value = "";
    ph.textContent = t("select_provider_placeholder", "— select provider —");
    sel.appendChild(ph);

    for (const k of keys) {
      const p = map[k] || {};
      const opt = document.createElement("option");
      opt.value = String(k);
      opt.textContent = String(p.name || k);
      sel.appendChild(opt);
    }

    if (oldValue && map[oldValue]) {
      sel.value = oldValue;
    } else {
      sel.value = "";
    }
  }

  function applyPreset(presetId) {
    if (!presetId) return;

    const auth = qByName("auth_type");
    const authType = (auth ? auth.value : "") || "LOGIN";
    const map = activePresetMap(authType);
    const p = map[presetId];
    if (!p) return;

    const host = qByName("host");
    const port = qByName("port");
    const sec = qByName("security");

    if (host && p.host != null) host.value = String(p.host);
    if (port && p.port != null) port.value = String(p.port);
    if (sec && p.security != null) sec.value = normSecurity(p.security);
  }

  function setAuth(v) {
    const auth = qByName("auth_type");
    if (auth) auth.value = v;

    const manual = byId("yyLoginBlock");
    const save = byId("yySaveWrap");
    const oauthG = byId("yyOauthGoogleBlock");
    const oauthM = byId("yyOauthMicrosoftBlock");

    const btnL = byId("yyAuthBtnLogin");
    const btnR = byId("yyAuthBtnRelay");
    const btnG = byId("yyAuthBtnGoogle");
    const btnM = byId("yyAuthBtnMicrosoft");

    const title = byId("yyManualModeTitle");
    const userPassRow = byId("yyLoginUserPassRow");

    const isLogin = (v === "LOGIN");
    const isRelay = (v === "RELAY_NOAUTH");
    const isManual = isLogin || isRelay;
    const isG = (v === "GOOGLE_OAUTH_2_0");
    const isM = (v === "MICROSOFT_OAUTH_2_0");

    show(manual, isManual);
    show(save, isManual);
    show(oauthG, isG);
    show(oauthM, isM);
    show(userPassRow, isLogin);

    if (title) {
      title.textContent = isRelay ? "RELAY NOAUTH" : t("smtp_login_standard", "LOGIN (Standard)");
    }

    setBtnStateClass(btnL, isLogin);
    setBtnStateClass(btnR, isRelay);
    setBtnStateClass(btnG, isG);
    setBtnStateClass(btnM, isM);

    if (isManual) refillPresetOptions(v);

    if (isRelay) {
      const sel = byId("yyPresetSelect");
      const sec = qByName("security");
      if (sec && sel && !String(sel.value || "").trim()) {
        sec.value = "none";
      }
    }
  }

  window.addEventListener("DOMContentLoaded", () => {
    const auth = qByName("auth_type");
    setAuth(auth ? (auth.value || "LOGIN") : "LOGIN");

    const btnL = byId("yyAuthBtnLogin");
    const btnR = byId("yyAuthBtnRelay");
    const btnG = byId("yyAuthBtnGoogle");
    const btnM = byId("yyAuthBtnMicrosoft");

    if (btnL) btnL.addEventListener("click", () => setAuth("LOGIN"));
    if (btnR) btnR.addEventListener("click", () => setAuth("RELAY_NOAUTH"));
    if (btnG) btnG.addEventListener("click", () => setAuth("GOOGLE_OAUTH_2_0"));
    if (btnM) btnM.addEventListener("click", () => setAuth("MICROSOFT_OAUTH_2_0"));

    const sel = byId("yyPresetSelect");
    if (sel) sel.addEventListener("change", () => applyPreset((sel.value || "").trim()));
  });
})();
