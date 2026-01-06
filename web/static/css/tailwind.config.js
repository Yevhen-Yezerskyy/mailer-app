/* FILE: web/static/css/tailwind.config.js  (новое — 2026-01-06)
 * PURPOSE: Tailwind build config for Serenity Mailer (Django).
 *          Scans all templates/static JS + tw_classmap.txt, adds safelist for dynamic classes.
 */
/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./web/**/*.html",
    "./web/**/*.js",
    "./web/mailer_web/tw_classmap.txt",
  ],
  safelist: [
    // твои рейтинговые bg-*
    { pattern: /^bg-(10|20|30|40|50|60|70|80|90|100)$/ },

    // иногда ты используешь important-модификатор в шаблонах
    { pattern: /^!text-\[.*\]$/ },
    { pattern: /^!mb-\d+$/ },

    // calc() для max-h / max-w (встречается в модалке)
    { pattern: /^(max-h|max-w)-\[calc\(.*\)\]$/ },
  ],
  theme: { extend: {} },
  plugins: [],
};
