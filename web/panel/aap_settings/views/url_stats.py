# FILE: web/panel/aap_settings/views/url_stats.py
# PATH: web/panel/aap_settings/views/url_stats.py
# DATE: 2026-01-31
# SUMMARY:
# - Settings → URL stats: static page with 2 embed-code variants (.de / .com) + RU instructions

from django.shortcuts import redirect, render
from django.utils.translation import gettext as _


def url_stats_view(request):
    if not request.user.is_authenticated:
        return redirect("public:login")

    snippet_de = """<script>
(function () {
  var v = (new URLSearchParams(window.location.search)).get('smrel');
  if (!v) return;
  var img = new Image();
  img.src = 'https://stat.serenity-mail.de/?smrel=' + encodeURIComponent(v);
})();
</script>"""

    snippet_com = """<script>
(function () {
  var v = (new URLSearchParams(window.location.search)).get('smrel');
  if (!v) return;
  var img = new Image();
  img.src = 'https://stat.serenity-mail.com/?smrel=' + encodeURIComponent(v);
})();
</script>"""

    ctx = {
        "page_title": _("Настройки : Учет переходов URL"),
        "snippet_de": snippet_de,
        "snippet_com": snippet_com,
    }
    return render(request, "panels/aap_settings/url_stats.html", ctx)
