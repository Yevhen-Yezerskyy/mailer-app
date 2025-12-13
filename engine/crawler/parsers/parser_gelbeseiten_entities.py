# FILE: engine/crawler/parsers/parser_gelbeseiten_entities.py  (новое) 2025-12-13

import re
from scrapy.http import Response


_RE_INT = re.compile(r"\d+")


def parse_entities_count(response: Response) -> int | None:
    """
    Вытаскивает число из:
    <span id="mod-TrefferlisteInfo">79486</span>
    """
    raw = response.css("span#mod-TrefferlisteInfo::text").get()
    if not raw:
        return None

    m = _RE_INT.search(raw.replace(".", "").replace(" ", ""))
    if not m:
        return None

    try:
        return int(m.group(0))
    except Exception:
        return None
