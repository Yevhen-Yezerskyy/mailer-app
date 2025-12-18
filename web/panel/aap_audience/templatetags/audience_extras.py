# FILE: web/aap_audience/templatetags/audience_extras.py  (новое) 2025-12-15
# Add: template filter get_item для доступа к dict по ключу в шаблонах.

from django import template

register = template.Library()


@register.filter
def get_item(d, key):
    try:
        return (d or {}).get(key)
    except Exception:
        return None
