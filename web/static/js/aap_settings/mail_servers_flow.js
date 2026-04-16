(function () {
  function syncFlowSteps(root) {
    const buttons = Array.from(root.querySelectorAll("[data-mail-flow-step-btn]"));
    if (!buttons.length) return;

    buttons.forEach((btn) => {
      btn.style.width = "250px";
    });
  }

  function syncAll() {
    document.querySelectorAll("[data-mail-flow-steps]").forEach(syncFlowSteps);
  }

  function bindMailLimitsGuard() {
    const form = document.getElementById("yySmtpForm");
    if (!form) return;

    const limitHour = form.querySelector('input[name="limit_hour"]');
    const limitDay = form.querySelector('input[name="limit_day"]');
    if (!limitHour || !limitDay) return;

    form.addEventListener("submit", function (e) {
      const submitter = e.submitter || document.activeElement;
      const action = submitter && submitter.getAttribute ? (submitter.getAttribute("value") || "").toLowerCase() : "";
      if (action && action !== "save") return;

      const hourEmpty = !(limitHour.value || "").trim();
      const dayEmpty = !(limitDay.value || "").trim();
      if (!hourEmpty && !dayEmpty) return;

      e.preventDefault();
      if (hourEmpty) {
        limitHour.focus();
        if (typeof limitHour.reportValidity === "function") limitHour.reportValidity();
        return;
      }
      limitDay.focus();
      if (typeof limitDay.reportValidity === "function") limitDay.reportValidity();
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      syncAll();
      bindMailLimitsGuard();
    });
  } else {
    syncAll();
    bindMailLimitsGuard();
  }

  window.addEventListener("resize", syncAll);
})();
