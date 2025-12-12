# FILE: engine/work/cities/plz.py  (новое) 2025-12-12

import csv
import sys
from collections import defaultdict

from engine.common.db import get_connection


def load_plz_csv(path: str):
    """
    CSV формата:
    ,lat,lng
    01067,51.0575...,13.7170...
    """
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        # пропускаем заголовок ",lat,lng"
        _header = next(reader, None)

        for r in reader:
            if len(r) < 3:
                continue

            plz_raw = (r[0] or "").strip()
            lat_raw = r[1]
            lng_raw = r[2]

            if not plz_raw or not lat_raw or not lng_raw:
                continue

            try:
                lat = float(lat_raw)
                lon = float(lng_raw)
            except ValueError:
                continue

            plz = plz_raw.zfill(5)
            rows.append({"plz": plz, "lat": lat, "lon": lon})

    return rows


def main(csv_path: str):
    plz_rows = load_plz_csv(csv_path)
    if not plz_rows:
        print("Нет валидных строк в CSV, выходим", file=sys.stderr)
        return

    print(f"Загружено PLZ строк: {len(plz_rows)}", file=sys.stderr)

    # gemeinde_id -> set(plz)
    mapping: dict[int, set[str]] = defaultdict(set)

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Проверка на пропущенные координаты
            cur.execute(
                "SELECT COUNT(*) FROM geo_gemeinden "
                "WHERE lat IS NULL OR lon IS NULL;"
            )
            missing = cur.fetchone()[0]
            if missing:
                print(
                    f"WARNING: {missing} geo_gemeinden без lat/lon, "
                    f"они не участвуют в привязке PLZ",
                    file=sys.stderr,
                )

            for i, row in enumerate(plz_rows, start=1):
                # один простой SELECT на каждый PLZ
                cur.execute(
                    """
                    SELECT id
                    FROM geo_gemeinden
                    WHERE lat IS NOT NULL AND lon IS NOT NULL
                    ORDER BY ((lat - %s)^2 + (lon - %s)^2)
                    LIMIT 1;
                    """,
                    (row["lat"], row["lon"]),
                )
                res = cur.fetchone()
                if res:
                    gid = res[0]
                    mapping[gid].add(row["plz"])

                if i % 1000 == 0:
                    print(f"… обработано {i} PLZ", file=sys.stderr)

            print(
                f"Gemeinden с хотя бы одним PLZ: {len(mapping)}",
                file=sys.stderr,
            )

            data = [
                (sorted(plz_set), gid)
                for gid, plz_set in mapping.items()
            ]

            print(
                f"Обновляем geo_gemeinden.plz_list для {len(data)} строк",
                file=sys.stderr,
            )

            cur.executemany(
                "UPDATE geo_gemeinden "
                "SET plz_list = %s "
                "WHERE id = %s;",
                data,
            )
            conn.commit()

    print("Готово: plz_list обновлён", file=sys.stderr)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(
            "Использование: python -m engine.work.cities.plz path/to/plz_geocoord.csv",
            file=sys.stderr,
        )
        sys.exit(1)

    main(sys.argv[1])
