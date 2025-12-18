# FILE: web/translate.py  (обновлено — 2025-12-18)
# PURPOSE:
# - run makemessages (incremental) for ru/de/uk
# - translate ONLY empty msgstr in TARGET languages (de/uk)
# - supports multiline msgid/msgstr (blocktrans produces msgid "")
# - keeps existing translations untouched
# - compilemessages at the end

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from engine.common.gpt import GPTClient


PROJECT_ROOT = Path(__file__).resolve().parent
LOCALE_DIR = PROJECT_ROOT / "locale"

SOURCE_LANG = "ru"
TARGET_LANGS = ["de", "uk"]
ALL_LANGS = [SOURCE_LANG] + TARGET_LANGS

SYSTEM_PROMPT = """You are a professional technical translator.

Rules:
- Translate from Russian into the target language.
- Preserve meaning exactly.
- No marketing, no embellishment.
- Keep punctuation and casing natural.
- Return ONLY the translated text, no quotes, no explanations.
"""


def run_manage(cmd: list[str]) -> None:
    subprocess.check_call([sys.executable, "manage.py", *cmd], cwd=str(PROJECT_ROOT))


def run_makemessages() -> None:
    args = ["makemessages"]
    for l in ALL_LANGS:
        args += ["-l", l]
    run_manage(args)


def run_compilemessages() -> None:
    run_manage(["compilemessages"])


def po_path(lang: str) -> Path:
    return LOCALE_DIR / lang / "LC_MESSAGES" / "django.po"


def _unquote_po(s: str) -> str:
    # input like: "text with \"quotes\""
    s = s.strip()
    if not (len(s) >= 2 and s[0] == '"' and s[-1] == '"'):
        return ""
    s = s[1:-1]
    return s.replace(r"\\", "\\").replace(r"\"", '"').replace(r"\n", "\n").replace(r"\t", "\t")


def _quote_po(s: str) -> str:
    s = s.replace("\\", r"\\").replace('"', r"\"").replace("\t", r"\t")
    # msgfmt любит \n в виде escape внутри строки
    s = s.replace("\n", r"\n")
    return f'"{s}"'


@dataclass
class PoEntry:
    prefix_lines: List[str]          # comments/refs/blank lines before msgid
    msgid_lines: List[str]           # msgid + continuations
    msgstr_lines: List[str]          # msgstr + continuations
    suffix_lines: List[str]          # any extra lines inside entry (rare)


def parse_po(text: str) -> tuple[List[str], List[PoEntry]]:
    """
    Returns (header_lines, entries)
    header_lines includes the initial header entry (msgid "" / msgstr "" ...) AND any lines before first real entry.
    Entries are subsequent msgid/msgstr blocks.
    """
    lines = text.splitlines(keepends=True)

    header: List[str] = []
    entries: List[PoEntry] = []

    i = 0

    # collect everything until we hit a non-header entry start after the first header block
    # we treat first msgid as header and keep it entirely in header_lines
    # Approach: keep lines until we see a second "msgid " that is not msgid "" in the file.
    seen_first_msgid = False
    in_first_entry = False

    while i < len(lines):
        line = lines[i]
        if line.startswith("msgid "):
            if not seen_first_msgid:
                seen_first_msgid = True
                in_first_entry = True
                header.append(line)
                i += 1
                continue
            # second msgid => header ends, start entries parsing from here
            if in_first_entry:
                # finish header
                break
        header.append(line)
        i += 1

    # parse remaining entries
    def collect_prefix(idx: int) -> tuple[List[str], int]:
        pref: List[str] = []
        while idx < len(lines) and not lines[idx].startswith("msgid "):
            pref.append(lines[idx])
            idx += 1
        return pref, idx

    def collect_field(idx: int, start_kw: str) -> tuple[List[str], int]:
        out: List[str] = []
        if idx < len(lines) and lines[idx].startswith(start_kw):
            out.append(lines[idx])
            idx += 1
            # continuations: lines that start with a quote
            while idx < len(lines) and lines[idx].lstrip().startswith('"'):
                out.append(lines[idx])
                idx += 1
        return out, idx

    while i < len(lines):
        prefix, i = collect_prefix(i)
        if i >= len(lines):
            break
        msgid_lines, i = collect_field(i, "msgid ")
        msgstr_lines, i = collect_field(i, "msgstr ")
        suffix: List[str] = []
        # collect any weird lines until next msgid or EOF
        while i < len(lines) and not lines[i].startswith("msgid "):
            # keep inside entry, but do not eat comments that belong to next entry (they’d be in prefix normally)
            suffix.append(lines[i])
            i += 1
        if msgid_lines:
            entries.append(PoEntry(prefix, msgid_lines, msgstr_lines, suffix))
        else:
            # stray lines
            header.extend(prefix)

    return header, entries


def msgid_text(entry: PoEntry) -> str:
    # msgid "..." + possible continuation quoted lines
    if not entry.msgid_lines:
        return ""
    first = entry.msgid_lines[0]
    if not first.startswith("msgid "):
        return ""
    text = _unquote_po(first[len("msgid "):])
    for cont in entry.msgid_lines[1:]:
        text += _unquote_po(cont)
    return text


def msgstr_is_empty(entry: PoEntry) -> bool:
    if not entry.msgstr_lines:
        return True
    # consider empty if all parts are ""
    first = entry.msgstr_lines[0]
    if not first.startswith("msgstr "):
        return True
    text = _unquote_po(first[len("msgstr "):])
    for cont in entry.msgstr_lines[1:]:
        text += _unquote_po(cont)
    return text == ""


def set_msgstr(entry: PoEntry, new_text: str) -> None:
    # store as one-line msgstr "...." (safe for msgfmt, even if contains \n)
    entry.msgstr_lines = [f"msgstr {_quote_po(new_text)}\n"]


def translate_text(client: GPTClient, src_ru: str, target_lang: str) -> str:
    resp = client.ask(
        tier="mini",
        workspace_id="system",
        user_id="translate",
        system=SYSTEM_PROMPT + f"\nTarget language: {target_lang}",
        user=src_ru,
        with_web=False,
        endpoint="i18n-translate",
    )
    return resp.content.strip()


def process_lang(lang: str) -> None:
    if lang == SOURCE_LANG:
        return

    path = po_path(lang)
    if not path.exists():
        print(f"[SKIP] {lang}: no django.po")
        return

    header, entries = parse_po(path.read_text(encoding="utf-8"))

    client = GPTClient(debug=False)
    changed = False

    for e in entries:
        if not msgstr_is_empty(e):
            continue
        src = msgid_text(e).strip()
        if not src:
            continue
        tr = translate_text(client, src, lang)
        set_msgstr(e, tr)
        changed = True

    if not changed:
        print(f"[SKIP] {lang}: nothing new")
        return

    out: List[str] = []
    out.extend(header)
    for e in entries:
        out.extend(e.prefix_lines)
        out.extend(e.msgid_lines)
        if e.msgstr_lines:
            out.extend(e.msgstr_lines)
        else:
            out.append('msgstr ""\n')
        out.extend(e.suffix_lines)

    path.write_text("".join(out), encoding="utf-8")
    print(f"[OK] {lang}: translated new strings")


def main() -> None:
    print("[1/3] makemessages (incremental)")
    run_makemessages()

    print("[2/3] translate empty msgstr (de/uk only, multiline supported)")
    for lang in TARGET_LANGS:
        process_lang(lang)

    print("[3/3] compilemessages")
    run_compilemessages()
    print("[DONE]")


if __name__ == "__main__":
    main()
