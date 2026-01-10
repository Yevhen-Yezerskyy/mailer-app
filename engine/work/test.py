# FILE: engine/work/run_denormalize_branches_prompt.py  (обновлено — 2026-01-08)
# PURPOSE: Прогоняет заданный немецкий текст через denormalize_branches_prompt() и печатает результат в stdout.

from engine.common.prompts.process import *



def main() -> None:
    out = get_prompt("prepare_branches_sell")
    print(out)


if __name__ == "__main__":
    main()
