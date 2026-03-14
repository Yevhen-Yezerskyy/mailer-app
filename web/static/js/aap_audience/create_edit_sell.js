// FILE: web/static/js/aap_audience/create_edit_sell.js
// DATE: 2026-03-14
// PURPOSE: Unsaved changes warnings for create/edit sell page using YYModal.

(function () {
  const root = document.getElementById("yy-create-edit-sell-config");
  if (!root) return;

  const labels = {
    title: root.dataset.labelTitle || "Title",
    product: root.dataset.labelProduct || "Product",
    company: root.dataset.labelCompany || "Company",
    geo: root.dataset.labelGeo || "Geo",
  };
  const forms = Array.from(document.querySelectorAll("[data-dirty-form]"));
  let allowUnload = false;

  function isWaitAction(submitter) {
    const action = submitter && submitter.name === "action" ? String(submitter.value || "") : "";
    return action === "suggest_title" || action === "process_product" || action === "process_company" || action === "process_geo";
  }

  function norm(value) {
    return String(value || "").replace(/\r\n/g, "\n").trim();
  }

  function fieldDirty(field) {
    return norm(field.value) !== norm(field.dataset.savedValue || "");
  }

  function formDirty(form) {
    return Array.from(form.querySelectorAll("[data-track-dirty='1']")).some(fieldDirty);
  }

  function anyDirty() {
    return forms.some(formDirty);
  }

  function dirtyFormNames(exceptForm) {
    return forms
      .filter(function (form) {
        return form !== exceptForm && formDirty(form);
      })
      .map(function (form) {
        return labels[form.getAttribute("data-dirty-form")] || form.getAttribute("data-dirty-form");
      });
  }

  function dirtyFormNamesForSubmit(form, submitter) {
    const action = submitter && submitter.name === "action" ? String(submitter.value || "") : "";

    if (
      action === "process_product" ||
      action === "process_company" ||
      action === "process_geo" ||
      action === "save_product" ||
      action === "save_company" ||
      action === "save_geo" ||
      action === "save_title" ||
      action === "reset_product_context" ||
      action === "reset_company_context" ||
      action === "reset_geo_context"
    ) {
      return dirtyFormNames(form);
    }

    return dirtyFormNames(null);
  }

  function esc(text) {
    return String(text || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function openDirtyModal(names, onContinue) {
    const tpl = document.getElementById("yy-unsaved-changes-template");
    if (!tpl) {
      if (window.confirm(names.join("\n"))) {
        allowUnload = true;
        onContinue();
      }
      return;
    }

    const wrapper = document.createElement("div");
    wrapper.innerHTML = tpl.innerHTML;
    const list = wrapper.querySelector("[data-unsaved-list]");
    const continueBtn = wrapper.querySelector("[data-unsaved-continue]");

    if (list) {
      list.innerHTML = names.map(function (name) {
        return "<li>" + esc(name) + "</li>";
      }).join("");
    }

    if (window.YYModal && typeof window.YYModal.open === "function") {
      window.YYModal.open("text=" + wrapper.innerHTML);
      const bind = function () {
        const btn = document.querySelector("#yy-modal-body [data-unsaved-continue]");
        if (!btn) {
          window.setTimeout(bind, 0);
          return;
        }
        btn.addEventListener("click", function () {
          if (window.YYModal && typeof window.YYModal.close === "function") {
            window.YYModal.close();
          }
          allowUnload = true;
          onContinue();
        }, { once: true });
      };
      bind();
      return;
    }

    if (window.confirm(modalText + "\n\n" + names.join("\n"))) {
      allowUnload = true;
      onContinue();
    }
  }

  forms.forEach(function (form) {
    form.addEventListener("submit", function (event) {
      const submitter = event.submitter;
      const names = dirtyFormNamesForSubmit(form, event.submitter);
      if (!names.length) {
        if (isWaitAction(submitter) && window.YYWaitModal) window.YYWaitModal.open();
        return;
      }
      event.preventDefault();
      openDirtyModal(names, function () {
        if (isWaitAction(submitter) && window.YYWaitModal) window.YYWaitModal.open();
        form.submit();
      });
    });
  });

  document.querySelectorAll("[data-submit-role='nav']").forEach(function (form) {
    form.addEventListener("submit", function (event) {
      const names = dirtyFormNames(null);
      if (!names.length) return;
      event.preventDefault();
      openDirtyModal(names, function () { form.submit(); });
    });
  });

  document.querySelectorAll("[data-dirty-nav]").forEach(function (button) {
    button.addEventListener("click", function () {
      const names = dirtyFormNames(null);
      if (!names.length) {
        allowUnload = true;
        window.location.href = button.getAttribute("data-dirty-nav");
        return;
      }
      openDirtyModal(names, function () {
        window.location.href = button.getAttribute("data-dirty-nav");
        });
      });
    });
})();
