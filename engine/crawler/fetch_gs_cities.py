# FILE: engine/work/fix_gb_branches_slug_decode.py  (новое) 2025-12-13

from __future__ import annotations

from urllib.parse import unquote

from engine.common.db import get_connection


def norm_slug(s: str) -> str:
    # канон: декодированный slug
    # (НЕ lower(), чтобы не ломать возможные кейсы; если хочешь lower — скажешь)
    return unquote(s).strip()


def main():
    # 1) загрузили все
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, slug, entities FROM gb_branches ORDER BY id")
            rows = cur.fetchall()

    # 2) сгруппировали по нормализованному slug
    groups: dict[str, list[tuple[int, str, str, int | None]]] = {}
    for _id, name, slug, entities in rows:
        ns = norm_slug(slug or "")
        if not ns:
            continue
        groups.setdefault(ns, []).append((_id, name, slug, entities))

    # 3) обработали дубли: оставляем минимальный id, entities = max, name = лучший (самый "человечный")
    merged = 0
    deleted = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            for ns, items in groups.items():
                if len(items) == 1:
                    # просто нормализуем slug если надо
                    _id, name, slug, entities = items[0]
                    if slug != ns:
                        cur.execute("UPDATE gb_branches SET slug=%s WHERE id=%s", (ns, _id))
                    continue

                items_sorted = sorted(items, key=lambda x: x[0])
                keep_id, keep_name, keep_slug, keep_entities = items_sorted[0]

                # выбрать лучший name: предпочитаем тот, где slug уже не содержит '%'
                def name_score(it):
                    _id, name, slug, entities = it
                    score = 0
                    if "%" not in (slug or ""):
                        score += 10
                    score += len(name or "")
                    return score

                best = max(items_sorted, key=name_score)
                best_name = best[1] or keep_name
                best_entities = max([e for (_, _, _, e) in items_sorted if e is not None], default=None)

                # обновить keep
                cur.execute(
                    "UPDATE gb_branches SET slug=%s, name=%s, entities=%s WHERE id=%s",
                    (ns, best_name, best_entities, keep_id),
                )

                # удалить остальные
                kill_ids = [i[0] for i in items_sorted[1:]]
                cur.execute("DELETE FROM gb_branches WHERE id = ANY(%s)", (kill_ids,))

                merged += 1
                deleted += len(kill_ids)
                print(f"[merge] slug='{ns}' keep={keep_id} deleted={len(kill_ids)}")

        conn.commit()

    print(f"\nDONE. merged_groups={merged}, deleted_rows={deleted}")

    # 4) поставить уникальность на slug (если ещё нет)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_indexes
                        WHERE schemaname='public'
                          AND indexname='gb_branches_slug_key'
                    ) THEN
                        -- если у тебя уже есть UNIQUE(slug), этот блок можно убрать
                        BEGIN
                            ALTER TABLE gb_branches ADD CONSTRAINT gb_branches_slug_key UNIQUE (slug);
                        EXCEPTION WHEN duplicate_object THEN
                            NULL;
                        END;
                    END IF;
                END$$;
            """)
        conn.commit()

    print("[ok] unique(slug) ensured")


if __name__ == "__main__":
    main()
