// FILE: web/static/js/confirm_modal.js
// DATE: 2026-04-13
// PURPOSE: Replace browser confirm() with project modal for forms marked by data-yy-confirm.

(function () {
  let pendingForm = null;

  function esc(s) {
    return String(s || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function openConfirmModal(message, approveLabel, cancelLabel) {
    const html =
      '<div class="YY-CARD_WHITE overflow-hidden" style="max-height:85vh;width:620px;">' +
      '<button type="button" class="YY-BUTTON_MAIN !w-fit" data-yy-modal-close style="position:absolute;top:10px;right:10px;">✕</button>' +
      '<div class="overflow-auto mt-10 mb-1 max-h-[calc(85vh-5rem)]">' +
      '<div class="YY-STATUS_YELLOW !mb-3">' + esc(message) + "</div>" +
      '<div class="grid grid-cols-2 gap-4">' +
      '<button type="button" class="YY-BUTTON_MAIN_FULL" data-yy-confirm-approve="1">' + esc(approveLabel) + "</button>" +
      '<button type="button" class="YY-BUTTON_MAIN_FULL" data-yy-modal-close data-yy-confirm-cancel="1">' + esc(cancelLabel) + "</button>" +
      "</div>" +
      "</div>" +
      "</div>";

    if (window.YYModal && typeof window.YYModal.open === "function") {
      window.YYModal.open("text=" + html);
    }
  }

  document.addEventListener(
    "click",
    function (e) {
      const submitter = e.target.closest(
        "form[data-yy-confirm] button[type='submit'], form[data-yy-confirm] input[type='submit']"
      );
      if (!submitter) return;

      const form = submitter.form || submitter.closest("form[data-yy-confirm]");
      if (!form) return;

      if (form.dataset.yyConfirmBypass === "1") {
        delete form.dataset.yyConfirmBypass;
        return;
      }

      e.preventDefault();
      pendingForm = form;

      const message = form.getAttribute("data-yy-confirm") || "Подтвердить действие?";
      const approveLabel = form.getAttribute("data-yy-confirm-approve") || "Подтвердить";
      const cancelLabel = form.getAttribute("data-yy-confirm-cancel") || "Отмена";
      openConfirmModal(message, approveLabel, cancelLabel);
    },
    true
  );

  document.addEventListener(
    "click",
    function (e) {
      const approveBtn = e.target.closest("[data-yy-confirm-approve]");
      if (approveBtn) {
        e.preventDefault();
        const form = pendingForm;
        pendingForm = null;
        if (!form) return;
        if (window.YYModal && typeof window.YYModal.close === "function") {
          window.YYModal.close();
        }
        form.dataset.yyConfirmBypass = "1";
        HTMLFormElement.prototype.submit.call(form);
        return;
      }

      const cancelBtn = e.target.closest("[data-yy-confirm-cancel]");
      if (cancelBtn) {
        pendingForm = null;
      }
    },
    true
  );
})();
