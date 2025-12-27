# FILE: engine/scripts/translate_gb_branches_i18n.py
# DATE: 2025-12-27
# PURPOSE:
#   Перевести немецкие названия бизнес-категорий из gb_branches.name
#   на RU / UK / EN через GPT (model=mini) и ДОБАВИТЬ в gb_branch_i18n.
#   ВАЖНО: никаких update / upsert — только INSERT. Если запись уже есть, пропускаем.
#
# RUN:
#   ./.venv/bin/python -m engine.scripts.translate_gb_branches_i18n
#   ./.venv/bin/python -m engine.scripts.translate_gb_branches_i18n --langs ru uk en --limit 200
#

from __future__ import annotations

import argparse
import re
import time
from typing import List, Tuple

from engine.common.db import get_connection
from engine.common.gpt import GPTClient


TARGET_LANGS_DEFAULT = ("ru", "uk", "en")

LANG_LABEL = {
    "ru": "Russian",
    "uk": "Ukrainian",
    "en": "English",
}

SYSTEM_PROMPT = (
    "You are a professional translator.\n\n"
    "Context:\n"
    "- Input is a BUSINESS CATEGORY name from a German online directory "
    "(Gelbe Seiten / Yellow Pages).\n"
    "- The category is used in a B2B lead generation system.\n\n"
    "Task:\n"
    "- Translate the German category name into {LANG}.\n"
    "- Use a short, neutral, business-appropriate translation.\n"
    "- Do NOT explain.\n"
    "- Do NOT add quotes.\n"
    "- Output ONLY the translated category name."
)


def fetch_branches(limit: int | None) -> List[Tuple[int, str]]:
    sql = """
        SELECT id, name
        FROM public.gb_branches
        ORDER BY id
    """
    if limit:
        sql += " LIMIT %s"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,) if limit else ())
            return [(int(r[0]), str(r[1] or "").strip()) for r in cur.fetchall()]


def translation_exists(branch_id: int, lang: str) -> bool:
    sql = """
        SELECT 1
        FROM public.gb_branch_i18n
        WHERE branch_id = %s AND lang = %s
        LIMIT 1
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (branch_id, lang))
            return cur.fetchone() is not None


def insert_translation(*, branch_id: int, lang: str, name_original: str, name_trans: str) -> None:
    sql = """
        INSERT INTO public.gb_branch_i18n
            (branch_id, lang, name_original, name_trans, created_at, updated_at)
        VALUES
            (%s, %s, %s, %s, now(), now())
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (branch_id, lang, name_original, name_trans))
        conn.commit()


def clean_text(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "\"'":
        s = s[1:-1].strip()
    return s


def translate(gpt: GPTClient, *, text_de: str, lang: str) -> str:
    instructions = SYSTEM_PROMPT.format(LANG=LANG_LABEL[lang])
    inp = f"German category: {text_de}"

    resp = gpt.ask(
        instructions=instructions,
        input=inp,
        service_tier="flex",
        user_id="system",
        model="gpt-5-mini",
    )

    out = clean_text(resp.content or "")
    if not out:
        raise RuntimeError("Empty GPT response")
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--langs", nargs="+", default=list(TARGET_LANGS_DEFAULT))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--sleep", type=float, default=0.0)
    args = ap.parse_args()

    langs = [str(l).strip().lower() for l in (args.langs or []) if str(l).strip()]
    langs = [l for l in langs if l in LANG_LABEL]
    if not langs:
        raise SystemExit("No valid langs provided (ru/uk/en).")

    branches = fetch_branches(args.limit)
    print(f"Branches loaded: {len(branches)} | langs={langs}")

    gpt = GPTClient()

    inserted = 0
    skipped = 0
    errors = 0

    for branch_id, name_de in branches:
        if not name_de:
            continue

        for lang in langs:
            if translation_exists(branch_id, lang):
                skipped += 1
                continue

            try:
                tr = translate(gpt, text_de=name_de, lang=lang)
                insert_translation(
                    branch_id=branch_id,
                    lang=lang,
                    name_original=name_de,
                    name_trans=tr,
                )
                inserted += 1
                print(f"[OK] {branch_id=} {lang=}  '{name_de}' -> '{tr}'")
            except Exception as e:
                errors += 1
                print(f"[ERR] {branch_id=} {lang=} '{name_de}' :: {e}")

            if args.sleep:
                time.sleep(float(args.sleep))

    print(f"INSERTED={inserted}  SKIPPED={skipped}  ERRORS={errors}")


if __name__ == "__main__":
    main()
