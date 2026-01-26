# FILE: web/translate.py  (обновлено — 2026-01-26)
# PURPOSE:
# - (0) PRE-CLEAN .po for target langs (de/uk/en):
#     - delete ALL duplicate entries by key (msgctxt+msgid+msgid_plural) entirely (every duplicate block)
#     - delete entries with empty msgstr (forces makemessages to re-add cleanly)
#     - delete entries with broken python-format placeholders (forces re-translate)
# - (1) makemessages (incremental) for ru/de/uk/en
# - (2) ALWAYS remove "#, fuzzy" from all .po
# - (3) POST-CLEAN target langs again (duplicates/broken format ONLY; MUST NOT drop empty after makemessages)
# - (4) translate ALL empty/suspicious entries for target langs (de/uk/en) with strict placeholder safety
#     - 3 passes: pass1 uses cache, pass2/pass3 disable cache to "dodge" cached empties/bad outputs
#     - after final pass prints leftover empty/problem entries with key + problem details
# - (5) compilemessages (always, even if some entries remain empty)
#
# CHANGE (2026-01-26):
# - FIX: Step (3) no longer deletes empty msgstr. After makemessages, new entries are empty by design;
#        deleting empties at step (3) removed all entries and made candidates=0.
# - ADD: multi-pass translation (cache on then off) + detailed leftover report.

from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
- Do not translate or change international tech words like SMTP, IMAP, Email, E-mail etc but keep and integrate in the context correctly
- Do all translations in context of SaaS mailer app
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
    text = _unquote_po(head[len(kw) :])
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
    msgstr_lines: List[str] = field(default_factory=list)  # singular
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

    # 1) duplicates by key => delete ALL
    buckets: Dict[Tuple[str, str, str], List[int]] = {}
    for i, e in enumerate(entries):
        key = _entry_key(e)
        if key[1] == "":
            continue
        buckets.setdefault(key, []).append(i)

    to_delete = set()
    for idxs in buckets.values():
        if len(idxs) > 1:
            to_delete.update(idxs)

    # 2) optionally delete empty translations
    if drop_empty:
        for i, e in enumerate(entries):
            key = _entry_key(e)
            if key[1] == "":
                continue
            if _is_empty_translation(e, nplurals):
                to_delete.add(i)

    # 3) delete broken python-format translations
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


def _gpt_translate(client: GPTClient, src: str, lang: str, *, use_cache: bool) -> str:
    resp = client.ask(
        model="mini",
        user_id="system",
        instructions=SYSTEM_PROMPT + f"\nTarget language: {lang}",
        input=src,
        use_cache=use_cache,
    )
    return (resp.content or "").strip()


def _translate_safe(
    client: GPTClient,
    src: str,
    lang: str,
    guard: bool,
    *,
    use_cache: bool,
    attempts: int = 2,
) -> Tuple[str, str]:
    """
    returns: (translation_or_empty, reason)
    reason: ok | gpt_empty | bad_token | specs_mismatch
    """
    src0 = (src or "").strip()
    if not src0:
        return ("", "gpt_empty")

    if not guard:
        tr0 = _gpt_translate(client, src0, lang, use_cache=use_cache)
        if not tr0:
            return ("", "gpt_empty")
        if _BAD_TOKEN_RE.search(tr0):
            return ("", "bad_token")
        return (tr0, "ok")

    frozen, specs = _freeze_specs(src0)

    last_reason = "gpt_empty"
    for _ in range(max(1, int(attempts))):
        tr = _gpt_translate(client, frozen, lang, use_cache=use_cache)
        if not tr:
            last_reason = "gpt_empty"
            continue

        tr = _restore_specs(tr, specs)

        if _BAD_TOKEN_RE.search(tr):
            last_reason = "bad_token"
            continue

        if _specs_match(src0, tr):
            return (tr, "ok")

        last_reason = "specs_mismatch"

    return ("", last_reason)


# ---------------- reporting ----------------


@dataclass
class LeftoverIssue:
    lang: str
    key: Tuple[str, str, str]  # (msgctxt, msgid, msgid_plural)
    problem: str  # empty | bad_token | specs_mismatch | gpt_empty
    plural_index: Optional[int] = None
    current_msgstr: str = ""
    src_specs: List[str] = field(default_factory=list)
    dst_specs: List[str] = field(default_factory=list)


def _print_leftovers(issues: List[LeftoverIssue]) -> None:
    if not issues:
        return
    print("[WARN] Leftover issues after translation passes:")
    for it in issues:
        ctxt, msgid, plural = it.key
        ctx_s = f"msgctxt={ctxt!r} " if ctxt else ""
        plur_s = f" msgid_plural={plural!r}" if plural else ""
        pi_s = f" plural[{it.plural_index}]" if it.plural_index is not None else ""
        print(f"- lang={it.lang}{pi_s} problem={it.problem} {ctx_s}msgid={msgid!r}{plur_s}")
        if it.src_specs or it.dst_specs:
            print(f"  specs src={it.src_specs} dst={it.dst_specs}")
        if it.current_msgstr.strip():
            print(f"  current={it.current_msgstr!r}")
        else:
            print("  current=<EMPTY>")


# ---------------- translate pass ----------------


def _translate_lang_once(lang: str, *, pass_no: int, use_cache: bool) -> Tuple[int, int, int, List[LeftoverIssue]]:
    """
    returns: (candidates, translated_ok, left_empty_or_bad, leftover_issues)
    """
    if lang == SOURCE_LANG:
        return (0, 0, 0, [])

    p = po_path(lang)
    if not p.exists():
        return (0, 0, 0, [])

    header, entries = parse_po(p.read_text(encoding="utf-8"))
    nplurals = _get_nplurals_from_header(header)
    client = GPTClient()

    total = ok = left = 0
    changed = False
    issues: List[LeftoverIssue] = []

    for e in entries:
        msgid = _field_text(e.msgid_lines, "msgid ").strip()
        if not msgid:
            continue
        if msgid == "":
            continue  # header entry

        key = _entry_key(e)
        guard = _needs_guard(e, msgid)

        if e.msgid_plural_lines:
            plural = _field_text(e.msgid_plural_lines, "msgid_plural ").strip() or msgid
            if not e.msgstrn_lines:
                for k in range(nplurals):
                    e.msgstrn_lines[k] = [f'msgstr[{k}] ""\n']
                e.msgstr_lines = []

            for k in range(nplurals):
                src = msgid if k == 0 else plural
                kw = f"msgstr[{k}] "
                cur = _field_text(e.msgstrn_lines.get(k, []), kw)

                suspicious = (
                    not (cur or "").strip()
                    or (guard and not _specs_match(src, cur))
                    or bool(_BAD_TOKEN_RE.search(cur or ""))
                )
                if not suspicious:
                    continue

                total += 1
                tr, reason = _translate_safe(
                    client,
                    src,
                    lang,
                    guard,
                    use_cache=use_cache,
                    attempts=2 if pass_no == 1 else 4,
                )
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
                    issues.append(
                        LeftoverIssue(
                            lang=lang,
                            key=key,
                            problem=reason if reason else "gpt_empty",
                            plural_index=k,
                            current_msgstr=(cur or "").strip(),
                            src_specs=_extract_specs(src),
                            dst_specs=_extract_specs(cur or ""),
                        )
                    )
            continue

        cur = _field_text(e.msgstr_lines, "msgstr ")
        suspicious = (
            not (cur or "").strip()
            or (guard and not _specs_match(msgid, cur))
            or bool(_BAD_TOKEN_RE.search(cur or ""))
        )
        if not suspicious:
            continue

        total += 1
        tr, reason = _translate_safe(
            client,
            msgid,
            lang,
            guard,
            use_cache=use_cache,
            attempts=2 if pass_no == 1 else 4,
        )
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
            issues.append(
                LeftoverIssue(
                    lang=lang,
                    key=key,
                    problem=reason if reason else "gpt_empty",
                    plural_index=None,
                    current_msgstr=(cur or "").strip(),
                    src_specs=_extract_specs(msgid),
                    dst_specs=_extract_specs(cur or ""),
                )
            )

    if changed:
        p.write_text(serialize_po(header, entries), encoding="utf-8")

    return (total, ok, left, issues)


def _final_scan_leftovers(lang: str) -> List[LeftoverIssue]:
    """
    Scan final .po and report any empty or broken-format msgstr (only printf-guarded).
    """
    p = po_path(lang)
    if not p.exists():
        return []

    header, entries = parse_po(p.read_text(encoding="utf-8"))
    nplurals = _get_nplurals_from_header(header)

    out: List[LeftoverIssue] = []
    for e in entries:
        msgid = _field_text(e.msgid_lines, "msgid ").strip()
        if not msgid or msgid == "":
            continue
        key = _entry_key(e)
        guard = _needs_guard(e, msgid)

        if e.msgid_plural_lines:
            plural = _field_text(e.msgid_plural_lines, "msgid_plural ").strip() or msgid
            for k in range(nplurals):
                src = msgid if k == 0 else plural
                kw = f"msgstr[{k}] "
                cur = _field_text(e.msgstrn_lines.get(k, []), kw)
                if not (cur or "").strip():
                    out.append(
                        LeftoverIssue(
                            lang=lang,
                            key=key,
                            problem="empty",
                            plural_index=k,
                            current_msgstr="",
                            src_specs=_extract_specs(src),
                            dst_specs=[],
                        )
                    )
                    continue
                if guard and not _specs_match(src, cur):
                    out.append(
                        LeftoverIssue(
                            lang=lang,
                            key=key,
                            problem="specs_mismatch",
                            plural_index=k,
                            current_msgstr=(cur or "").strip(),
                            src_specs=_extract_specs(src),
                            dst_specs=_extract_specs(cur or ""),
                        )
                    )
            continue

        cur = _field_text(e.msgstr_lines, "msgstr ")
        if not (cur or "").strip():
            out.append(
                LeftoverIssue(
                    lang=lang,
                    key=key,
                    problem="empty",
                    plural_index=None,
                    current_msgstr="",
                    src_specs=_extract_specs(msgid),
                    dst_specs=[],
                )
            )
            continue
        if guard and not _specs_match(msgid, cur):
            out.append(
                LeftoverIssue(
                    lang=lang,
                    key=key,
                    problem="specs_mismatch",
                    plural_index=None,
                    current_msgstr=(cur or "").strip(),
                    src_specs=_extract_specs(msgid),
                    dst_specs=_extract_specs(cur or ""),
                )
            )
    return out


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
    # IMPORTANT: MUST NOT drop empty here, because makemessages creates new entries with empty msgstr by design.
    for lang in TARGET_LANGS:
        _drop_duplicates_and_bad(lang, drop_empty=False)

    print("[4/6] translate empty/suspicious msgstr (de/uk/en) — 3 passes")
    totals: Dict[str, Tuple[int, int, int]] = {}
    leftovers_all: List[LeftoverIssue] = []

    passes = [
        (1, True),   # pass1: cache ON
        (2, False),  # pass2: cache OFF
        (3, False),  # pass3: cache OFF
    ]

    for pass_no, use_cache in passes:
        print(f"  [pass {pass_no}/3] use_cache={use_cache}")
        for lang in TARGET_LANGS:
            total, ok, left, issues = _translate_lang_once(lang, pass_no=pass_no, use_cache=use_cache)
            totals[lang] = (total, ok, left)
            print(f"    - {lang}: candidates={total} translated={ok} left={left}")
            # NOTE: keep issues only for visibility; final scan below is the source of truth.
            leftovers_all.extend(issues)

    print("[5/6] compilemessages (always)")
    run_compilemessages()

    # Final scan & print real leftovers (after all passes)
    final_issues: List[LeftoverIssue] = []
    for lang in TARGET_LANGS:
        final_issues.extend(_final_scan_leftovers(lang))
    if final_issues:
        _print_leftovers(final_issues)
        print("[WARN] Some entries are still empty or have placeholder mismatch after 3 passes.")
        print("[HINT] Usually: API/key/rate-limit/error OR model keeps breaking placeholders. Re-run later.")
    else:
        print("[OK] No empty/mismatched entries detected in target .po files after 3 passes.")

    print("[DONE]--")


if __name__ == "__main__":
    main()
