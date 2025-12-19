# FILE: web/translate.py  (обновлено — 2025-12-19)
# PURPOSE:
# - run makemessages (incremental) for ru/de/uk
# - translate ONLY empty msgstr in TARGET languages (de/uk)
# - ALWAYS remove "#, fuzzy" flags from all .po files
# - supports multiline msgid/msgstr (blocktrans produces msgid "")
# - keeps existing translations untouched
# - ALWAYS compilemessages at the end

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List

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


# ---------- fuzzy cleanup ----------

def strip_fuzzy_in_po_file(po_path: Path) -> None:
    text = po_path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    out: list[str] = []
    for line in lines:
        if line.startswith("#,"):
            flags = [f.strip() for f in line[2:].split(",") if f.strip()]
            flags = [f for f in flags if f != "fuzzy"]
            if not flags:
                continue
            out.append("#, " + ", ".join(flags) + "\n")
        else:
            out.append(line)

    po_path.write_text("".join(out), encoding="utf-8")


def strip_fuzzy_all() -> None:
    for lang in ALL_LANGS:
        path = po_path(lang)
        if path.exists():
            strip_fuzzy_in_po_file(path)


# ---------- PO parsing ----------

def _unquote_po(s: str) -> str:
    s = s.strip()
    if not (len(s) >= 2 and s[0] == '"' and s[-1] == '"'):
        return ""
    s = s[1:-1]
    return (
        s.replace(r"\\", "\\")
         .replace(r"\"", '"')
         .replace(r"\n", "\n")
         .replace(r"\t", "\t")
    )


def _quote_po(s: str) -> str:
    s = s.replace("\\", r"\\").replace('"', r"\"").replace("\t", r"\t")
    s = s.replace("\n", r"\n")
    return f'"{s}"'


@dataclass
class PoEntry:
    prefix_lines: List[str]
    msgid_lines: List[str]
    msgstr_lines: List[str]
    suffix_lines: List[str]


def parse_po(text: str) -> tuple[List[str], List[PoEntry]]:
    lines = text.splitlines(keepends=True)

    header: List[str] = []
    entries: List[PoEntry] = []

    i = 0
    seen_first_msgid = False

    while i < len(lines):
        line = lines[i]
        if line.startswith("msgid "):
            if not seen_first_msgid:
                seen_first_msgid = True
                header.append(line)
                i += 1
                continue
            break
        header.append(line)
        i += 1

    def collect_prefix(idx: int):
        out = []
        while idx < len(lines) and not lines[idx].startswith("msgid "):
            out.append(lines[idx])
            idx += 1
        return out, idx

    def collect_field(idx: int, kw: str):
        out = []
        if idx < len(lines) and lines[idx].startswith(kw):
            out.append(lines[idx])
            idx += 1
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
        suffix = []
        while i < len(lines) and not lines[i].startswith("msgid "):
            suffix.append(lines[i])
            i += 1
        if msgid_lines:
            entries.append(PoEntry(prefix, msgid_lines, msgstr_lines, suffix))

    return header, entries


def msgid_text(e: PoEntry) -> str:
    if not e.msgid_lines:
        return ""
    text = _unquote_po(e.msgid_lines[0][len("msgid "):])
    for l in e.msgid_lines[1:]:
        text += _unquote_po(l)
    return text


def msgstr_is_empty(e: PoEntry) -> bool:
    if not e.msgstr_lines:
        return True
    text = _unquote_po(e.msgstr_lines[0][len("msgstr "):])
    for l in e.msgstr_lines[1:]:
        text += _unquote_po(l)
    return text == ""


def set_msgstr(e: PoEntry, text: str) -> None:
    e.msgstr_lines = [f"msgstr {_quote_po(text)}\n"]


def translate_text(client: GPTClient, src: str, lang: str) -> str:
    resp = client.ask(
        tier="mini",
        workspace_id="system",
        user_id="translate",
        system=SYSTEM_PROMPT + f"\nTarget language: {lang}",
        user=src,
        with_web=False,
        endpoint="i18n-translate",
    )
    return resp.content.strip()


def process_lang(lang: str) -> None:
    if lang == SOURCE_LANG:
        return

    path = po_path(lang)
    if not path.exists():
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
        set_msgstr(e, translate_text(client, src, lang))
        changed = True

    if not changed:
        return

    out: List[str] = []
    out.extend(header)
    for e in entries:
        out.extend(e.prefix_lines)
        out.extend(e.msgid_lines)
        out.extend(e.msgstr_lines or ['msgstr ""\n'])
        out.extend(e.suffix_lines)

    path.write_text("".join(out), encoding="utf-8")


def main() -> None:
    print("[1/4] makemessages")
    run_makemessages()

    print("[2/4] strip fuzzy")
    strip_fuzzy_all()

    print("[3/4] translate empty msgstr (de/uk)")
    for lang in TARGET_LANGS:
        process_lang(lang)

    print("[4/4] compilemessages")
    run_compilemessages()
    print("[DONE]")


if __name__ == "__main__":
    main()
