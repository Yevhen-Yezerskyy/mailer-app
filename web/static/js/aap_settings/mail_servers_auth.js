// FILE: web/static/js/aap_settings/mail_servers_auth.js
// DATE: 2026-01-23
// PURPOSE: Settings → Mail servers (SMTP/IMAP): переключение login vs google/microsoft oauth2.
// CHANGE:
// - Connect href выставляется реально: form[data-oauth-google-url] / form[data-oauth-microsoft-url] + next.
// - Панель проверок SMTP/IMAP скрывается в OAuth режиме, показывается подсказка.

(function () {
  function byId(id) { return document.getElementById(id); }

  function isOauth(v) {
    return v === "google_oauth2" || v === "microsoft_oauth2";
  }

  function connectBaseUrl(authType) {
    const form = byId("yyMailServerForm");
    if (!form) return "";
    if (authType === "google_oauth2") return (form.dataset.oauthGoogleUrl || "").trim();
    if (authType === "microsoft_oauth2") return (form.dataset.oauthMicrosoftUrl || "").trim();
    return "";
  }

  function currentNextUrl() {
    const form = byId("yyMailServerForm");
    const u = form ? (form.dataset.nextUrl || "").trim() : "";
    return u || window.location.pathname + window.location.search;
  }

  function updateConnectHref(authType) {
    const btn = byId("yyConnectBtn");
    if (!btn) return;

    const base = connectBaseUrl(authType);
    if (!base) {
      btn.href = "#";
      btn.onclick = function (e) { e.preventDefault(); };
      return;
    }

    const next = encodeURIComponent(currentNextUrl());
    btn.href = base + (base.includes("?") ? "&" : "?") + "next=" + next;
    btn.onclick = null;
  }

  function toggleChecksForAuth(authType) {
    const wrap = byId("yyProtoChecksWrap");
    const hint = byId("yyProtoChecksOauthHint");
    if (!wrap || !hint) return;

    if (isOauth(authType)) {
      wrap.classList.add("hidden");
      hint.classList.remove("hidden");
    } else {
      hint.classList.add("hidden");
      wrap.classList.remove("hidden");
    }
  }

  function sync() {
    const auth = byId("yyAuthType");
    const manual = byId("yyManualBlock");
    const oauth = byId("yyOauthBlock");
    if (!auth || !manual || !oauth) return;

    const v = (auth.value || "").trim();

    if (isOauth(v)) {
      manual.classList.add("hidden");
      oauth.classList.remove("hidden");
      updateConnectHref(v);
    } else {
      oauth.classList.add("hidden");
      manual.classList.remove("hidden");
    }

    toggleChecksForAuth(v);
  }

  window.addEventListener("DOMContentLoaded", () => {
    const auth = byId("yyAuthType");
    if (!auth) return;
    auth.addEventListener("change", sync);
    sync();
  });
})();
