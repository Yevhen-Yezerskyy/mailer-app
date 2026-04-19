// FILE: web/static/js/campaign_templates/campaign_letters/submit.js
// DATE: 2026-01-20
// PURPOSE: Submit + unsaved guard: собрать editor_html + subjects + headers_json,
//          и предупреждать при закрытии (верх/низ) если письмо изменено и не сохранено.

(function () {
  "use strict";

  const $ = (s) => document.querySelector(s);
  const i18n = window.yyI18n || (document.documentElement && document.documentElement.yyI18n) || {};
  const t = (key, fallback) => {
    const v = i18n[key];
    return typeof v === "string" && v.trim() ? v : fallback;
  };

  function normalizeTabsTo2Spaces(s) {
    return (s || "").replace(/\t/g, "  ");
  }

  function normalizeComparable(s) {
    return normalizeTabsTo2Spaces(String(s || "").replace(/\r\n/g, "\n")).trim();
  }

  function openGuardModal(opts) {
    const title = String((opts && opts.title) || "");
    const text = String((opts && opts.text) || "");
    const statusClass = String((opts && opts.statusClass) || "YY-STATUS_BLUE");
    const approveLabel = String((opts && opts.approveLabel) || t("continue_label", "Continue"));
    const cancelLabel = String((opts && opts.cancelLabel) || t("cancel", "Cancel"));
    const onApprove = opts && typeof opts.onApprove === "function" ? opts.onApprove : null;
    const showCancel = (opts && opts.showCancel) !== false;

    if (!window.YYModal || typeof window.YYModal.open !== "function") {
      return false;
    }

    const q = new URLSearchParams();
    q.set("title", title);
    q.set("text", text);
    q.set("status", statusClass);
    q.set("approve", approveLabel);
    q.set("cancel", cancelLabel);
    q.set("show_cancel", showCancel ? "1" : "0");
    window.YYModal.open("url=/panel/campaigns/campaigns/letter/modal/guard/?" + q.toString());

    if (!onApprove) {
      return true;
    }
    const bind = function () {
      const btn = document.querySelector("#yy-modal-body [data-yy-letter-guard-approve]");
      if (!btn) {
        window.setTimeout(bind, 0);
        return;
      }
      btn.addEventListener("click", function () {
        if (window.YYModal && typeof window.YYModal.close === "function") {
          window.YYModal.close();
        }
        onApprove();
      }, { once: true });
    };
    bind();
    return true;
  }

  function getMode() {
    const el = $("#yyEditorMode");
    const v = el ? String(el.value || "").trim() : "user";
    return v === "advanced" ? "advanced" : "user";
  }

  function getUserEditorHtml() {
    try {
      const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
      return ed ? (ed.getContent({ format: "html" }) || "") : "";
    } catch (_) {
      return "";
    }
  }

  function getAdvHtml() {
    try {
      if (typeof window.yyCampEnsureCodeMirror === "function") window.yyCampEnsureCodeMirror();
      return typeof window.yyCampAdvGetHtml === "function" ? (window.yyCampAdvGetHtml() || "") : "";
    } catch (_) {
      return "";
    }
  }

  function getCurrentContentForCompare() {
    const mode = getMode();
    if (mode === "advanced") return getAdvHtml();
    try {
      if (window.YYCampaignLetterTiny && typeof window.YYCampaignLetterTiny.getContentHtml === "function") {
        const v = window.YYCampaignLetterTiny.getContentHtml() || "";
        if (String(v || "").trim()) return String(v);
      }
    } catch (_) {}
    return getUserEditorHtml();
  }

  function setErr(el, isErr) {
    if (!el) return;
    if (isErr) {
      el.dataset.yyErr = "1";
      el.style.borderColor = "#ef4444";
      el.style.boxShadow = "0 0 0 2px rgba(239, 68, 68, 0.20)";
    } else {
      delete el.dataset.yyErr;
      el.style.borderColor = "";
      el.style.boxShadow = "";
    }
  }

  function bindClearOnInput(el) {
    if (!el) return;
    el.addEventListener("input", function () {
      const v = String(el.value || "").trim();
      setErr(el, !v);
    });
    el.addEventListener("blur", function () {
      const v = String(el.value || "").trim();
      setErr(el, !v);
    });
  }

  function validateSubjectsRequired() {
    const s1 = $("#yySubject1");
    const s2 = $("#yySubject2");
    const s3 = $("#yySubject3");

    const v1 = s1 ? String(s1.value || "").trim() : "";
    const v2 = s2 ? String(s2.value || "").trim() : "";
    const v3 = s3 ? String(s3.value || "").trim() : "";

    setErr(s1, !v1);
    setErr(s2, !v2);
    setErr(s3, !v3);

    if (!v1 || !v2 || !v3) {
      const firstBad = (!v1 && s1) || (!v2 && s2) || (!v3 && s3) || null;
      try { firstBad && firstBad.focus(); } catch (_) {}
      return null;
    }

    return [v1, v2, v3];
  }

  function getHeadersJsonText(mode) {
    // В visual-mode поле не видно => не затираем, берём init/hidden.
    if (mode !== "advanced") {
      const src = $("#yyInitHeaders");
      return String((src && src.value) || "{}");
    }

    try {
      if (typeof window.yyCampEnsureCodeMirror === "function") window.yyCampEnsureCodeMirror();
      if (typeof window.yyCampHeadersGet === "function") return String(window.yyCampHeadersGet() || "{}");
    } catch (_) {}

    const ta = $("#yyHeadersJsonArea");
    return String((ta && ta.value) || "{}");
  }

  function parseInitSubjects3() {
    const src = $("#yyInitSubjects");
    let arr = [];
    try {
      const parsed = JSON.parse(String((src && src.value) || "[]"));
      arr = Array.isArray(parsed) ? parsed : [];
    } catch (_) {}
    return [
      String(arr[0] || "").trim(),
      String(arr[1] || "").trim(),
      String(arr[2] || "").trim(),
    ];
  }

  function readCurrentSubjects3() {
    const s1 = $("#yySubject1");
    const s2 = $("#yySubject2");
    const s3 = $("#yySubject3");
    return [
      String((s1 && s1.value) || "").trim(),
      String((s2 && s2.value) || "").trim(),
      String((s3 && s3.value) || "").trim(),
    ];
  }

  function openConfirmModal(opts) {
    const title = String((opts && opts.title) || t("campaign_letter", "Campaign letter"));
    const text = String((opts && opts.text) || "");
    const statusClass = String((opts && opts.statusClass) || "YY-STATUS_BLUE");
    const approveLabel = String((opts && opts.approveLabel) || t("continue_label", "Continue"));
    const cancelLabel = String((opts && opts.cancelLabel) || t("cancel", "Cancel"));
    const onApprove = opts && typeof opts.onApprove === "function" ? opts.onApprove : null;

    if (openGuardModal({
      title,
      text,
      statusClass,
      approveLabel,
      cancelLabel,
      onApprove,
      showCancel: true,
    })) {
      return;
    }

    if (window.confirm(text) && onApprove) onApprove();
  }

  function openInfoModal(opts) {
    const title = String((opts && opts.title) || t("campaign_letter", "Campaign letter"));
    const text = String((opts && opts.text) || "");
    const statusClass = String((opts && opts.statusClass) || "YY-STATUS_BLUE");
    if (openGuardModal({
      title,
      text,
      statusClass,
      approveLabel: t("ok_label", "OK"),
      onApprove: null,
      showCancel: false,
    })) {
      return;
    }
    window.alert(text);
  }

  function init() {
    const form = $("#yySendingForm");
    if (!form) return;
    const closeTopLink = $("#yyFlowCloseTopBtn");
    const closeBottomBtn = $("#yyLetterCloseBtn");
    const readyExists = String(form.dataset.letterReadyExists || "") === "1";
    const templateChanged = String(form.dataset.letterTemplateChanged || "") === "1";

    const hiddenHtml = $("#yyEditorHtml");
    const hiddenSubs = $("#yySubjectsJson");
    const hiddenHeaders = $("#yyHeadersJson");
    if (!hiddenHtml || !hiddenSubs || !hiddenHeaders) return;

    // default headers hidden сразу
    const initHeaders = $("#yyInitHeaders");
    hiddenHeaders.value = String((initHeaders && initHeaders.value) || "{}");

    // bind live clear
    bindClearOnInput($("#yySubject1"));
    bindClearOnInput($("#yySubject2"));
    bindClearOnInput($("#yySubject3"));

    let initialSnapshot = {
      content: normalizeComparable(String(($("#yyInitContent") && $("#yyInitContent").value) || "")),
      subjects: JSON.stringify(parseInitSubjects3()),
    };

    function currentSnapshot() {
      return {
        content: normalizeComparable(getCurrentContentForCompare()),
        subjects: JSON.stringify(readCurrentSubjects3()),
      };
    }

    function syncInitialSnapshotFromCurrent() {
      initialSnapshot = currentSnapshot();
    }

    function syncInitialSnapshotAfterTinyReady() {
      const maxAttempts = 40;
      let attempts = 0;
      const poll = function () {
        attempts += 1;
        const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
        const ready = !!(ed && ed.initialized);
        if (ready) {
          try {
            if (typeof ed.setDirty === "function") ed.setDirty(false);
          } catch (_) {}
          syncInitialSnapshotFromCurrent();
          return;
        }
        if (attempts >= maxAttempts) {
          syncInitialSnapshotFromCurrent();
          return;
        }
        window.setTimeout(poll, 50);
      };
      poll();
    }
    syncInitialSnapshotAfterTinyReady();

    function isDirtyByEditorAndSubjects() {
      const ed = window.tinymce ? window.tinymce.get("yyTinyEditor") : null;
      const tinyReady = !!(ed && ed.initialized);
      const mode = getMode();

      let contentDirty = false;
      if (mode === "advanced") {
        const nowContent = normalizeComparable(getCurrentContentForCompare());
        contentDirty = nowContent !== initialSnapshot.content;
      } else if (tinyReady && typeof ed.isDirty === "function") {
        try {
          contentDirty = !!ed.isDirty();
        } catch (_) {
          const nowContent = normalizeComparable(getCurrentContentForCompare());
          contentDirty = nowContent !== initialSnapshot.content;
        }
      } else {
        const nowContent = normalizeComparable(getCurrentContentForCompare());
        contentDirty = nowContent !== initialSnapshot.content;
      }

      const nowSubjects = JSON.stringify(readCurrentSubjects3());
      const subjectsDirty = nowSubjects !== initialSnapshot.subjects;
      return contentDirty || subjectsDirty;
    }

    function guardClose(proceedFn) {
      if (!readyExists) {
        openConfirmModal({
          title: t("unsaved_changes_title", "Unsaved changes"),
          statusClass: "YY-STATUS_RED",
          text: t(
            "unsaved_close_to_campaigns",
            "Letter is not saved. Changes will not be applied. Go to campaigns list?"
          ),
          onApprove: proceedFn,
        });
        return true;
      }
      if (templateChanged) {
        openConfirmModal({
          title: t("campaign_letter", "Campaign letter"),
          statusClass: "YY-STATUS_BLUE",
          text: t(
            "template_changed_save_to_apply",
            "Letter template has changed. Review the letter and save changes. Then the new template will be applied."
          ),
          onApprove: proceedFn,
        });
        return true;
      }
      if (isDirtyByEditorAndSubjects()) {
        openConfirmModal({
          title: t("unsaved_changes_title", "Unsaved changes"),
          statusClass: "YY-STATUS_RED",
          text: t(
            "unsaved_close_to_campaigns",
            "Letter is not saved. Changes will not be applied. Go to campaigns list?"
          ),
          onApprove: proceedFn,
        });
        return true;
      }
      return false;
    }

    if (closeTopLink) {
      closeTopLink.addEventListener("click", function (e) {
        const href = String(closeTopLink.getAttribute("href") || "").trim();
        if (!href) return;
        const handled = guardClose(function () { window.location.href = href; });
        if (!handled) return;
        e.preventDefault();
        e.stopPropagation();
      });
    }

    if (closeBottomBtn) {
      closeBottomBtn.addEventListener("click", function (e) {
        const href = closeTopLink ? String(closeTopLink.getAttribute("href") || "").trim() : "";
        if (!href) return;
        const handled = guardClose(function () { window.location.href = href; });
        if (!handled) {
          window.location.href = href;
          return;
        }
        e.preventDefault();
        e.stopPropagation();
      });
    }

    form.addEventListener("submit", function (e) {
      const btn = e.submitter || document.activeElement;
      const action = btn && btn.value ? String(btn.value).trim() : "";
      if (action === "send_test") {
        if (templateChanged) {
          e.preventDefault();
          e.stopPropagation();
          openInfoModal({
            title: t("campaign_letter", "Campaign letter"),
            statusClass: "YY-STATUS_BLUE",
            text: t(
              "template_changed_save_to_apply",
              "Letter template has changed. Review the letter and save changes. Then the new template will be applied."
            ),
          });
          return;
        }
        if (isDirtyByEditorAndSubjects()) {
          e.preventDefault();
          e.stopPropagation();
          openInfoModal({
            title: t("campaign_letter", "Campaign letter"),
            statusClass: "YY-STATUS_BLUE",
            text: t(
              "save_first_unsaved_changes",
              "There are unsaved changes in the letter. Save the letter first."
            ),
          });
          return;
        }
        return;
      }

      if (action !== "save_letter" && action !== "save_ready") return;

      const subs = validateSubjectsRequired();
      if (!subs) {
        e.preventDefault();
        return;
      }

      const mode = getMode();
      hiddenHtml.value = mode === "advanced" ? normalizeTabsTo2Spaces(getAdvHtml()) : (getUserEditorHtml() || "");
      hiddenSubs.value = JSON.stringify(subs);
      hiddenHeaders.value = getHeadersJsonText(mode);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
