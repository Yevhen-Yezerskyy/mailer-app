# FILE: engine/crawler/pipelines/pipeline_gelbeseiten_entities.py  (новое) 2025-12-13

from engine.common.db import get_connection


class EntitiesPipeline:
    """
    Получаем entities и пишем в gb_branches.entities.
    Печатаем только если реально обновили.
    """

    SQL_UPDATE_BY_ID = """
        UPDATE gb_branches
        SET entities = %s
        WHERE id = %s
        RETURNING id, slug, name, entities;
    """

    SQL_UPDATE_BY_SLUG = """
        UPDATE gb_branches
        SET entities = %s
        WHERE slug = %s
        RETURNING id, slug, name, entities;
    """

    def process_item(self, item, spider):
        entities = item.get("entities")
        if entities is None:
            return item

        branch_id = item.get("branch_id")
        slug = item.get("slug")

        with get_connection() as conn:
            with conn.cursor() as cur:
                if branch_id is not None:
                    cur.execute(self.SQL_UPDATE_BY_ID, (entities, branch_id))
                else:
                    cur.execute(self.SQL_UPDATE_BY_SLUG, (entities, slug))
                row = cur.fetchone()

        if row:
            _id, _slug, _name, _entities = row
            print(f"[gb_branches.entities] id={_id} | {_slug} | {_name} -> entities={_entities}")

        return item
