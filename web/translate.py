# FILE: web/translate.py
# DATE: 2026-01-20
# PURPOSE:
# - (0) PRE-CLEAN .po for target langs (de/uk/en):
#     - delete ALL duplicate entries by key (msgctxt+msgid+msgid_plural) entirely (every duplicate block)
#     - delete entries with empty msgstr (forces makemessages to re-add cleanly)
#     - delete entries with broken python-format placeholders (forces re-translate)
# - (1) makemessages (incremental) for ru/de/uk/en
# - (2) ALWAYS remove "#, fuzzy" from all .po
# - (3) POST-CLEAN target langs again (duplicates/empties/broken format) to ensure clean state after merges
# - (4) translate ALL empty/suspicious entries for target langs (de/uk/en) with strict placeholder safety
# - (5) compilemessages
#
# WHY THIS FIXES YOUR BUG:
# - Django/msgfmt can “pick” the wrong duplicate (often the empty/broken one), so UI shows Russian.
# - We DELETE duplicate blocks completely (not “set msgstr to empty”), so only one entry remains.

from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple

from engine.common.gpt import GPTClient

PROJECT_ROOT = Path(__file__).resolve().parent
LOCALE_DIR = PROJECT_ROOT / "locale"

SOURCE_LANG = "ru"
TARGET_LANGS = ["de", "uk", "en"]
ALL_LANGS = [SOURCE_LANG] + TARGET_LANGS

SYSTEM_PROMPT = """You are a professional technical translator.

Rules:
- Translate the given source text into the target language.
- Preserve meaning exactly.
- No marketing, no embellishment.
- Keep punctuation and casing natural.
- Keep ALL placeholders exactly (e.g. %s, %(name)s, %%).
- Do NOT change tokens like __FMT0__ if they appear.
- Return ONLY the translated text, no quotes, no explanations.
"""

# printf-style specs + literal %%
_PRINTF_SPEC_RE = re.compile(
    r"""
    %%                                            # literal percent
    |
    %\([^)]+\)[#0\- +]?\d*(?:\.\d+)?[hlL]?[diouxXeEfFgGcrs]   # named
    |
    %[ #0\-+]*\d*(?:\.\d+)?[hlL]?[diouxXeEfFgGcrs]            # positional
    """,
    re.VERBOSE,
)
_BAD_TOKEN_RE = re.compile(r"__FMT\d+__")


# ---------------- manage.py helpers ----------------

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


# ---------------- fuzzy cleanup ----------------

def strip_fuzzy_in_po_file(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
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

    path.write_text("".join(out), encoding="utf-8")


def strip_fuzzy_all() -> None:
    for lang in ALL_LANGS:
        p = po_path(lang)
        if p.exists():
            strip_fuzzy_in_po_file(p)


# ---------------- PO parsing ----------------

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
    s = (s or "").replace("\\", r"\\").replace('"', r"\"").replace("\t", r"\t")
    s = s.replace("\n", r"\n")
    return f'"{s}"'


def _field_text(lines: List[str], kw: str) -> str:
    if not lines:
        return ""
    head = lines[0]
    if not head.startswith(kw):
        return ""
    text = _unquote_po(head[len(kw):])
    for l in lines[1:]:
        text += _unquote_po(l)
    return text


def _get_nplurals_from_header(header_lines: List[str]) -> int:
    blob = "".join(header_lines)
    m = re.search(r"Plural-Forms:\s*[^\\n]*nplurals\s*=\s*(\d+)", blob, re.IGNORECASE)
    if not m:
        return 2
    try:
        n = int(m.group(1))
        return n if 1 <= n <= 6 else 2
    except Exception:
        return 2


@dataclass
class PoEntry:
    prefix_lines: List[str] = field(default_factory=list)
    msgctxt_lines: List[str] = field(default_factory=list)
    msgid_lines: List[str] = field(default_factory=list)
    msgid_plural_lines: List[str] = field(default_factory=list)
    msgstr_lines: List[str] = field(default_factory=list)              # singular
    msgstrn_lines: Dict[int, List[str]] = field(default_factory=dict)  # plural
    suffix_lines: List[str] = field(default_factory=list)


def parse_po(text: str) -> Tuple[List[str], List[PoEntry]]:
    lines = text.splitlines(keepends=True)
    header: List[str] = []
    entries: List[PoEntry] = []

    i = 0
    seen_first_msgid = False

    # keep header intact (includes first msgid "" block)
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

    def collect_prefix(idx: int) -> Tuple[List[str], int]:
        out: List[str] = []
        while idx < len(lines) and not lines[idx].startswith("msgid "):
            out.append(lines[idx])
            idx += 1
        return out, idx

    def collect_field(idx: int, kw: str) -> Tuple[List[str], int]:
        out: List[str] = []
        if idx < len(lines) and lines[idx].startswith(kw):
            out.append(lines[idx])
            idx += 1
            while idx < len(lines) and lines[idx].lstrip().startswith('"'):
                out.append(lines[idx])
                idx += 1
        return out, idx

    def collect_msgstr_variants(idx: int) -> Tuple[List[str], Dict[int, List[str]], int]:
        singular: List[str] = []
        plural: Dict[int, List[str]] = {}

        if idx < len(lines) and lines[idx].startswith("msgstr "):
            singular, idx = collect_field(idx, "msgstr ")
            return singular, plural, idx

        while idx < len(lines) and lines[idx].startswith("msgstr["):
            m = re.match(r"^msgstr\[(\d+)\]\s+", lines[idx])
            if not m:
                break
            k = int(m.group(1))
            kw = f"msgstr[{k}] "
            blk, idx = collect_field(idx, kw)
            plural[k] = blk
        return singular, plural, idx

    while i < len(lines):
        prefix, i = collect_prefix(i)
        if i >= len(lines):
            break

        msgctxt_lines, j = collect_field(i, "msgctxt ")
        if msgctxt_lines:
            i = j

        msgid_lines, i = collect_field(i, "msgid ")
        msgid_plural_lines, j = collect_field(i, "msgid_plural ")
        if msgid_plural_lines:
            i = j

        msgstr_lines, msgstrn_lines, i = collect_msgstr_variants(i)

        suffix: List[str] = []
        while i < len(lines) and not lines[i].startswith("msgid "):
            suffix.append(lines[i])
            i += 1

        if msgid_lines:
            entries.append(
                PoEntry(
                    prefix_lines=prefix,
                    msgctxt_lines=msgctxt_lines,
                    msgid_lines=msgid_lines,
                    msgid_plural_lines=msgid_plural_lines,
                    msgstr_lines=msgstr_lines,
                    msgstrn_lines=msgstrn_lines,
                    suffix_lines=suffix,
                )
            )

    return header, entries


def serialize_po(header: List[str], entries: List[PoEntry]) -> str:
    out: List[str] = []
    out.extend(header)
    for e in entries:
        out.extend(e.prefix_lines)
        out.extend(e.msgctxt_lines)
        out.extend(e.msgid_lines)
        out.extend(e.msgid_plural_lines)

        if e.msgid_plural_lines:
            if e.msgstrn_lines:
                for k in sorted(e.msgstrn_lines.keys()):
                    out.extend(e.msgstrn_lines[k] or [f'msgstr[{k}] ""\n'])
            else:
                out.append('msgstr[0] ""\n')
                out.append('msgstr[1] ""\n')
        else:
            out.extend(e.msgstr_lines or ['msgstr ""\n'])

        out.extend(e.suffix_lines)
    return "".join(out)


# ---------------- duplicate/empty/broken detection ----------------

def _extract_specs(s: str) -> List[str]:
    return [m.group(0) for m in _PRINTF_SPEC_RE.finditer(s or "")]


def _has_python_format_flag(e: PoEntry) -> bool:
    for ln in e.prefix_lines:
        if ln.startswith("#,") and "python-format" in ln:
            return True
    return False


def _needs_guard(e: PoEntry, src: str) -> bool:
    return _has_python_format_flag(e) or bool(_extract_specs(src))


def _specs_match(src: str, dst: str) -> bool:
    return _extract_specs(src) == _extract_specs(dst)


def _entry_key(e: PoEntry) -> Tuple[str, str, str]:
    ctxt = _field_text(e.msgctxt_lines, "msgctxt ")
    msgid = _field_text(e.msgid_lines, "msgid ")
    plural = _field_text(e.msgid_plural_lines, "msgid_plural ")
    return (ctxt, msgid, plural)


def _is_empty_translation(e: PoEntry, nplurals: int) -> bool:
    if e.msgid_plural_lines:
        # if any plural form non-empty -> not empty
        for k in range(nplurals):
            kw = f"msgstr[{k}] "
            cur = _field_text(e.msgstrn_lines.get(k, []), kw)
            if (cur or "").strip():
                return False
        return True
    cur = _field_text(e.msgstr_lines, "msgstr ")
    return not (cur or "").strip()


def _is_broken_format(e: PoEntry, nplurals: int) -> bool:
    msgid = _field_text(e.msgid_lines, "msgid ").strip()
    if not msgid or msgid == "":
        return False
    guard = _needs_guard(e, msgid)
    if not guard:
        return False

    if e.msgid_plural_lines:
        plural = _field_text(e.msgid_plural_lines, "msgid_plural ").strip() or msgid
        for k in range(nplurals):
            src = msgid if k == 0 else plural
            kw = f"msgstr[{k}] "
            dst = _field_text(e.msgstrn_lines.get(k, []), kw)
            if (dst or "").strip() and not _specs_match(src, dst):
                return True
        return False

    dst = _field_text(e.msgstr_lines, "msgstr ")
    if (dst or "").strip() and not _specs_match(msgid, dst):
        return True
    return False


def _drop_duplicates_and_bad(lang: str, *, drop_empty: bool) -> None:
    p = po_path(lang)
    if not p.exists():
        return

    header, entries = parse_po(p.read_text(encoding="utf-8"))
    nplurals = _get_nplurals_from_header(header)

    # 1) find duplicates by key (ctxt+msgid+plural) and mark ALL for deletion
    buckets: Dict[Tuple[str, str, str], List[int]] = {}
    for i, e in enumerate(entries):
        key = _entry_key(e)
        # ignore header entry msgid "" safely
        if key[1] == "":
            continue
        buckets.setdefault(key, []).append(i)

    to_delete = set()
    for key, idxs in buckets.items():
        if len(idxs) > 1:
            for i in idxs:
                to_delete.add(i)

    # 2) also delete empty translations (target langs) if requested
    if drop_empty:
        for i, e in enumerate(entries):
            key = _entry_key(e)
            if key[1] == "":
                continue
            if _is_empty_translation(e, nplurals):
                to_delete.add(i)

    # 3) delete broken python-format translations (force re-translate)
    for i, e in enumerate(entries):
        key = _entry_key(e)
        if key[1] == "":
            continue
        if _is_broken_format(e, nplurals):
            to_delete.add(i)

    if not to_delete:
        return

    kept = [e for i, e in enumerate(entries) if i not in to_delete]
    p.write_text(serialize_po(header, kept), encoding="utf-8")


# ---------------- GPT translation (placeholder-safe) ----------------

def _freeze_specs(src: str) -> Tuple[str, List[str]]:
    specs = _extract_specs(src)
    if not specs:
        return src, []
    idx = 0

    def repl(_m: re.Match) -> str:
        nonlocal idx
        tok = f"__FMT{idx}__"
        idx += 1
        return tok

    frozen = _PRINTF_SPEC_RE.sub(repl, src)
    return frozen, specs


def _restore_specs(text: str, specs: List[str]) -> str:
    out = text
    for i, spec in enumerate(specs):
        out = out.replace(f"__FMT{i}__", spec)
    return out


def _gpt_translate(client: GPTClient, src: str, lang: str) -> str:
    resp = client.ask(
        model="mini",
        user_id="system",
        instructions=SYSTEM_PROMPT + f"\nTarget language: {lang}",
        input=src,
    )
    return (resp.content or "").strip()


def _translate_safe(client: GPTClient, src: str, lang: str, guard: bool) -> str:
    src0 = (src or "").strip()
    if not src0:
        return ""

    if not guard:
        tr = _gpt_translate(client, src0, lang)
        return "" if _BAD_TOKEN_RE.search(tr or "") else (tr or "")

    frozen, specs = _freeze_specs(src0)

    for _ in range(2):  # 2 attempts
        tr = _gpt_translate(client, frozen, lang)
        if not tr:
            continue
        tr = _restore_specs(tr, specs)
        if _BAD_TOKEN_RE.search(tr):
            continue
        if _specs_match(src0, tr):
            return tr

    return ""


def _translate_lang(lang: str) -> Tuple[int, int, int]:
    """
    returns: (candidates, translated_ok, left_empty)
    """
    if lang == SOURCE_LANG:
        return (0, 0, 0)

    p = po_path(lang)
    if not p.exists():
        return (0, 0, 0)

    header, entries = parse_po(p.read_text(encoding="utf-8"))
    nplurals = _get_nplurals_from_header(header)
    client = GPTClient()

    total = ok = left = 0
    changed = False

    for e in entries:
        msgid = _field_text(e.msgid_lines, "msgid ").strip()
        if not msgid:
            continue
        if msgid == "":
            continue  # header entry

        guard = _needs_guard(e, msgid)

        if e.msgid_plural_lines:
            plural = _field_text(e.msgid_plural_lines, "msgid_plural ").strip() or msgid
            # ensure msgstr[n] blocks exist
            if not e.msgstrn_lines:
                for k in range(nplurals):
                    e.msgstrn_lines[k] = [f'msgstr[{k}] ""\n']
                e.msgstr_lines = []

            for k in range(nplurals):
                src = msgid if k == 0 else plural
                kw = f"msgstr[{k}] "
                cur = _field_text(e.msgstrn_lines.get(k, []), kw)
                suspicious = (not (cur or "").strip()) or (guard and not _specs_match(src, cur)) or bool(_BAD_TOKEN_RE.search(cur or ""))
                if not suspicious:
                    continue

                total += 1
                tr = _translate_safe(client, src, lang, guard)
                if tr:
                    e.msgstrn_lines[k] = [f"msgstr[{k}] {_quote_po(tr)}\n"]
                    e.msgstr_lines = []
                    changed = True
                    ok += 1
                else:
                    e.msgstrn_lines[k] = [f'msgstr[{k}] ""\n']
                    e.msgstr_lines = []
                    changed = True
                    left += 1
            continue

        cur = _field_text(e.msgstr_lines, "msgstr ")
        suspicious = (not (cur or "").strip()) or (guard and not _specs_match(msgid, cur)) or bool(_BAD_TOKEN_RE.search(cur or ""))
        if not suspicious:
            continue

        total += 1
        tr = _translate_safe(client, msgid, lang, guard)
        if tr:
            e.msgstr_lines = [f"msgstr {_quote_po(tr)}\n"]
            e.msgstrn_lines.clear()
            changed = True
            ok += 1
        else:
            e.msgstr_lines = ['msgstr ""\n']
            e.msgstrn_lines.clear()
            changed = True
            left += 1

    if changed:
        p.write_text(serialize_po(header, entries), encoding="utf-8")

    return (total, ok, left)


# ---------------- main ----------------

def main() -> None:
    print("[0/6] pre-clean target .po: delete duplicates + empty + broken-format")
    for lang in TARGET_LANGS:
        _drop_duplicates_and_bad(lang, drop_empty=True)

    print("[1/6] makemessages")
    run_makemessages()

    print("[2/6] strip fuzzy")
    strip_fuzzy_all()

    print("[3/6] post-clean target .po again (after makemessages merge)")
    for lang in TARGET_LANGS:
        _drop_duplicates_and_bad(lang, drop_empty=True)

    print("[4/6] translate empty/suspicious msgstr (de/uk/en)")
    totals: Dict[str, Tuple[int, int, int]] = {}
    for lang in TARGET_LANGS:
        total, ok, left = _translate_lang(lang)
        totals[lang] = (total, ok, left)
        print(f"  - {lang}: candidates={total} translated={ok} left_empty={left}")

    print("[5/6] compilemessages")
    run_compilemessages()

    bad = {k: v for k, v in totals.items() if v[2] > 0}
    if bad:
        print("[WARN] Some entries are still empty after translation:")
        for lang, (total, ok, left) in bad.items():
            print(f"  - {lang}: left_empty={left} (candidates={total}, translated={ok})")
        print("[HINT] Usually: GPT returned empty (API/key/rate-limit/error). Re-run after fixing.")
    print("[DONE]--")


if __name__ == "__main__":
    main()
