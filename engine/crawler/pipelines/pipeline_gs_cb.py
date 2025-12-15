# FILE: engine/crawler/pipelines/pipeline_gs_cb.py  (обновление) 2025-12-14
# Красивый JSON вывод перед UPSERT.

import json
from engine.common.db import get_connection


class GSCBPipeline:
    def open_spider(self, spider):
        self.conn = get_connection()
        self.cur = self.conn.cursor()

    def close_spider(self, spider):
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def process_item(self, item, spider):
        print(
            "DB UPSERT raw_contacts_gb:\n"
            + json.dumps(item, ensure_ascii=False, indent=2)
        )

        self.cur.execute(
            """
            INSERT INTO raw_contacts_gb
                (cb_crawler_id, company_name, email, company_data, created_at, updated_at)
            VALUES
                (%s, %s, %s, %s::jsonb, now(), now())
            ON CONFLICT (cb_crawler_id, company_name)
            DO UPDATE SET
                email = EXCLUDED.email,
                company_data = EXCLUDED.company_data,
                updated_at = now()
            """,
            (
                item["cb_crawler_id"],
                item["company_name"],
                item.get("email"),
                json.dumps(item.get("company_data", {}), ensure_ascii=False),
            ),
        )
        return item
