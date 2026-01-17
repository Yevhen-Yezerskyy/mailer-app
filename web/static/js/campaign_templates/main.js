// FILE: web/static/js/campaign_templates/main.js  (новое — 2026-01-17)
// PURPOSE: Оркестратор: init Tiny + стартовый режим + bind submit.

(function () {
  "use strict";

  function init() {
    const form = document.getElementById("yyTplForm");
    if (!form) return;

    window.YYCampaignTplTiny.initTiny();
    window.YYCampaignTplMode.showUserMode();
    window.YYCampaignTplSubmit.bindSubmit();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
