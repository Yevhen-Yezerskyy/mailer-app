# FILE: engine/crawler/pipelines/pipeline_gelbeseiten_branches.py  (новое) 2025-12-13

from engine.common.db import get_connection


class BranchesPipeline:
    """
    Кладём бранчи в gb_branches.
    - если slug новый → вставляем name, slug, num = 1
    - если slug уже есть → num = num + 1, name обновляем
    На каждом успешном upsert печатаем, что стало с num.
    """

    SQL = """
        INSERT INTO gb_branches (name, slug, num)
        VALUES (%s, %s, 1)
        ON CONFLICT (slug) DO UPDATE
        SET
            num  = gb_branches.num + 1,
            name = EXCLUDED.name
        RETURNING num;
    """

    def process_item(self, item, spider):
        name = item.get("branch_name_raw")
        slug = item.get("branch_slug")

        if not slug or not name:
            return item

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(self.SQL, (name, slug))
                row = cur.fetchone()

        if row is not None:
            num = row[0]
            print(f"[gb_branches] {slug} | {name} → num={num}")

        return item
