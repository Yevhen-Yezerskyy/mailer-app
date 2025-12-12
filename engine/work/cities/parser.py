# FILE: engine/work/cities/parser.py  (новое) 2025-12-12

from __future__ import annotations

from pathlib import Path

import pandas as pd

from engine.common.db import get_connection


# -------------------------------------------------------
# ПУТИ / НАСТРОЙКИ
# -------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
XLSX_PATH = BASE_DIR / "AuszugGV2QAktuell.xlsx"


# -------------------------------------------------------
# ХЕЛПЕРЫ
# -------------------------------------------------------

def _to_int(v):
    if pd.isna(v):
        return None
    try:
        return int(v)
    except Exception:
        return None


def _to_float(v):
    if pd.isna(v):
        return None
    if isinstance(v, str):
        v = v.replace(",", ".")
    try:
        return float(v)
    except Exception:
        return None


# -------------------------------------------------------
# ЧТЕНИЕ EXCEL
# -------------------------------------------------------

def load_raw_df() -> pd.DataFrame:
    """
    Читаем официальный Gemeindeverzeichnis:
      - лист Onlineprodukt_Gemeinden30062025
      - пропускаем 6 строк шапки
      - жёстко задаём 20 колонок
    """
    df = pd.read_excel(
        XLSX_PATH,
        sheet_name="Onlineprodukt_Gemeinden30062025",
        skiprows=6,
        header=None,
    )

    df.columns = [
        "satzart", "text_code",
        "ars_land", "ars_rb", "ars_kreis", "ars_vb", "ars_gem",
        "name",
        "area_km2",
        "pop_total", "pop_male", "pop_female", "pop_density",
        "plz",
        "lon", "lat",
        "travel_code", "travel_name",
        "urban_code", "urban_name",
    ]

    df["satzart"] = pd.to_numeric(df["satzart"], errors="coerce")
    df = df[df["satzart"].notna()]
    df["satzart"] = df["satzart"].astype(int)

    # ARS-колонки в числа
    for col in ["ars_land", "ars_rb", "ars_kreis", "ars_vb", "ars_gem"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# -------------------------------------------------------
# РАЗРЕЗ НА УРОВНИ ARS
# -------------------------------------------------------

def split_levels(df: pd.DataFrame):
    """
    satzart:
      10 — Bundesland (Land)
      40 — Kreis / kreisfreie Stadt
      50 — VB (Verbandsgemeinde / Amt / Samtgemeinde / etc.)
      60 — Gemeinde / Stadt

    RB (Regierungsbezirk) в файле отдельным satzart может быть (20/30),
    но надёжнее всего вытащить список RB из любых строк, где ars_rb != 0.
    """

    # -------- LAND (satzart = 10) --------
    states_df = df[df["satzart"] == 10].copy()
    # на всякий случай дедуп по ars_land
    states_df = (
        states_df
        .sort_values(["ars_land"])
        .drop_duplicates(subset=["ars_land"], keep="first")
    )

    # -------- REGIERUNGSBEZIRK (RB) --------
    regions_raw = df[df["ars_rb"].notna() & (df["ars_rb"] != 0)].copy()
    regions_raw = (
        regions_raw
        .sort_values(["ars_land", "ars_rb", "satzart"])
    )
    regions_df = (
        regions_raw
        .drop_duplicates(subset=["ars_land", "ars_rb"], keep="first")
    )

    # -------- KREISE (satzart = 40) --------
    districts_df = df[df["satzart"] == 40].copy()
    districts_df = (
        districts_df
        .sort_values(["ars_land", "ars_rb", "ars_kreis"])
        .drop_duplicates(subset=["ars_land", "ars_rb", "ars_kreis"], keep="first")
    )

    # -------- VERWALTUNGSEINHEITEN (satzart = 50) --------
    subdistricts_df = df[df["satzart"] == 50].copy()
    subdistricts_df = (
        subdistricts_df
        .sort_values(["ars_land", "ars_rb", "ars_kreis", "ars_vb"])
        .drop_duplicates(
            subset=["ars_land", "ars_rb", "ars_kreis", "ars_vb"],
            keep="first",
        )
    )

    # -------- GEMEINDEN (satzart = 60, с PLZ-агрегацией) --------
    cities_raw = df[df["satzart"] == 60].copy()

    def plz_to_str(v):
        if pd.isna(v):
            return None
        try:
            return f"{int(v):05d}"
        except Exception:
            return None

    cities_raw["plz_str"] = cities_raw["plz"].apply(plz_to_str)

    cities_df = (
        cities_raw
        .groupby(
            ["ars_land", "ars_rb", "ars_kreis", "ars_vb", "ars_gem"],
            as_index=False,
        )
        .agg({
            "name": "first",
            "area_km2": "first",
            "pop_total": "first",
            "pop_male": "first",
            "pop_female": "first",
            "pop_density": "first",
            "lon": "first",
            "lat": "first",
            "urban_code": "first",
            "urban_name": "first",
            "travel_code": "first",
            "travel_name": "first",
            "plz_str": lambda s: sorted({p for p in s if p}),
        })
    )

    cities_df.rename(columns={"plz_str": "plz_list"}, inplace=True)

    return states_df, regions_df, districts_df, subdistricts_df, cities_df


# -------------------------------------------------------
# МАПЫ НАЗВАНИЙ ДЛЯ ДЕНОРМАЛИЗАЦИИ
# -------------------------------------------------------

def build_maps(states_df, regions_df, districts_df, subdistricts_df):
    states_map = {
        _to_int(row["ars_land"]): row["name"]
        for _, row in states_df.iterrows()
    }

    regions_map = {
        (_to_int(row["ars_land"]), _to_int(row["ars_rb"])): row["name"]
        for _, row in regions_df.iterrows()
    }

    districts_map = {
        (_to_int(row["ars_land"]), _to_int(row["ars_rb"]), _to_int(row["ars_kreis"])): row["name"]
        for _, row in districts_df.iterrows()
    }

    subdistricts_map = {
        (
            _to_int(row["ars_land"]),
            _to_int(row["ars_rb"]),
            _to_int(row["ars_kreis"]),
            _to_int(row["ars_vb"]),
        ): row["name"]
        for _, row in subdistricts_df.iterrows()
    }

    return states_map, regions_map, districts_map, subdistricts_map


# -------------------------------------------------------
# ВСТАВКА В БД
# -------------------------------------------------------

def insert_states(states_df, conn):
    sql = """
        INSERT INTO geo_states (ars_land, name, area_km2, pop_total)
        VALUES (%s, %s, %s, %s)
    """
    rows = []
    for _, r in states_df.iterrows():
        rows.append(
            (
                _to_int(r["ars_land"]),
                r["name"],
                _to_float(r["area_km2"]),
                _to_int(r["pop_total"]),
            )
        )
    with conn.cursor() as cur:
        cur.executemany(sql, rows)


def insert_regions(regions_df, conn):
    sql = """
        INSERT INTO geo_regions (ars_land, ars_rb, name, area_km2, pop_total)
        VALUES (%s, %s, %s, %s, %s)
    """
    rows = []
    for _, r in regions_df.iterrows():
        rows.append(
            (
                _to_int(r["ars_land"]),
                _to_int(r["ars_rb"]),
                r["name"],
                _to_float(r["area_km2"]),
                _to_int(r["pop_total"]),
            )
        )
    with conn.cursor() as cur:
        cur.executemany(sql, rows)


def insert_districts(districts_df, conn):
    sql = """
        INSERT INTO geo_districts
        (ars_land, ars_rb, ars_kreis, name, area_km2, pop_total)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    rows = []
    for _, r in districts_df.iterrows():
        rows.append(
            (
                _to_int(r["ars_land"]),
                _to_int(r["ars_rb"]),
                _to_int(r["ars_kreis"]),
                r["name"],
                _to_float(r["area_km2"]),
                _to_int(r["pop_total"]),
            )
        )
    with conn.cursor() as cur:
        cur.executemany(sql, rows)


def insert_subdistricts(subdistricts_df, conn):
    sql = """
        INSERT INTO geo_subdistricts
        (ars_land, ars_rb, ars_kreis, ars_vb, name, area_km2, pop_total)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    rows = []
    for _, r in subdistricts_df.iterrows():
        rows.append(
            (
                _to_int(r["ars_land"]),
                _to_int(r["ars_rb"]),
                _to_int(r["ars_kreis"]),
                _to_int(r["ars_vb"]),
                r["name"],
                _to_float(r["area_km2"]),
                _to_int(r["pop_total"]),
            )
        )
    with conn.cursor() as cur:
        cur.executemany(sql, rows)


def insert_cities(cities_df, maps, conn):
    states_map, regions_map, districts_map, subdistricts_map = maps

    sql = """
        INSERT INTO geo_cities
        (ars_land, ars_rb, ars_kreis, ars_vb, ars_gem,
         state_name, region_name, district_name, subdistrict_name,
         name, area_km2, pop_total, pop_male, pop_female, pop_density,
         plz_list, lon, lat,
         urban_code, urban_name, travel_code, travel_name)
        VALUES (%s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s)
    """

    rows = []
    missing_regions = 0
    missing_districts = 0
    missing_subdistricts = 0

    for _, r in cities_df.iterrows():
        ars_land = _to_int(r["ars_land"])
        ars_rb = _to_int(r["ars_rb"])
        ars_kreis = _to_int(r["ars_kreis"])
        ars_vb = _to_int(r["ars_vb"])
        ars_gem = _to_int(r["ars_gem"])

        state_name = states_map.get(ars_land, f"Land {ars_land}")
        region_name = regions_map.get((ars_land, ars_rb))
        if ars_rb is not None and region_name is None:
            region_name = f"RB {ars_rb}"
            missing_regions += 1

        district_name = districts_map.get((ars_land, ars_rb, ars_kreis))
        if district_name is None:
            district_name = f"Kreis {ars_kreis}"
            missing_districts += 1

        subdistrict_name = None
        if ars_vb is not None and ars_vb != 0:
            subdistrict_name = subdistricts_map.get(
                (ars_land, ars_rb, ars_kreis, ars_vb)
            )
            if subdistrict_name is None:
                subdistrict_name = f"VB {ars_vb}"
                missing_subdistricts += 1

        plz_list = r["plz_list"] or []
        plz_list = [str(p) for p in plz_list]

        rows.append(
            (
                ars_land,
                ars_rb,
                ars_kreis,
                ars_vb,
                ars_gem,
                state_name,
                region_name,
                district_name,
                subdistrict_name,
                r["name"],
                _to_float(r["area_km2"]),
                _to_int(r["pop_total"]),
                _to_int(r["pop_male"]),
                _to_int(r["pop_female"]),
                _to_float(r["pop_density"]),
                plz_list,
                _to_float(r["lon"]),
                _to_float(r["lat"]),
                r["urban_code"],
                r["urban_name"],
                r["travel_code"],
                r["travel_name"],
            )
        )

    with conn.cursor() as cur:
        cur.executemany(sql, rows)

    print(f"\n[WARN] Города без явного region_name (RB): {missing_regions}")
    print(f"[WARN] Города без явного district_name:    {missing_districts}")
    print(f"[WARN] Города без явного subdistrict_name: {missing_subdistricts}")


# -------------------------------------------------------
# АГРЕГАЦИЯ ПЛОЩАДИ / НАСЕЛЕНИЯ НАВЕРХ
# -------------------------------------------------------

def aggregate_up(conn):
    with conn.cursor() as cur:
        # subdistricts из cities
        cur.execute("""
            UPDATE geo_subdistricts s
            SET
                area_km2  = agg.area_km2,
                pop_total = agg.pop_total
            FROM (
                SELECT
                    ars_land,
                    ars_rb,
                    ars_kreis,
                    ars_vb,
                    SUM(area_km2)  AS area_km2,
                    SUM(pop_total) AS pop_total
                FROM geo_cities
                GROUP BY ars_land, ars_rb, ars_kreis, ars_vb
            ) AS agg
            WHERE s.ars_land  = agg.ars_land
              AND s.ars_rb    = agg.ars_rb
              AND s.ars_kreis = agg.ars_kreis
              AND s.ars_vb    = agg.ars_vb;
        """)

        # districts из cities
        cur.execute("""
            UPDATE geo_districts d
            SET
                area_km2  = agg.area_km2,
                pop_total = agg.pop_total
            FROM (
                SELECT
                    ars_land,
                    ars_rb,
                    ars_kreis,
                    SUM(area_km2)  AS area_km2,
                    SUM(pop_total) AS pop_total
                FROM geo_cities
                GROUP BY ars_land, ars_rb, ars_kreis
            ) AS agg
            WHERE d.ars_land  = agg.ars_land
              AND d.ars_rb    = agg.ars_rb
              AND d.ars_kreis = agg.ars_kreis;
        """)

        # regions из cities
        cur.execute("""
            UPDATE geo_regions r
            SET
                area_km2  = agg.area_km2,
                pop_total = agg.pop_total
            FROM (
                SELECT
                    ars_land,
                    ars_rb,
                    SUM(area_km2)  AS area_km2,
                    SUM(pop_total) AS pop_total
                FROM geo_cities
                GROUP BY ars_land, ars_rb
            ) AS agg
            WHERE r.ars_land = agg.ars_land
              AND r.ars_rb   = agg.ars_rb;
        """)

        # states из cities
        cur.execute("""
            UPDATE geo_states s
            SET
                area_km2  = agg.area_km2,
                pop_total = agg.pop_total
            FROM (
                SELECT
                    ars_land,
                    SUM(area_km2)  AS area_km2,
                    SUM(pop_total) AS pop_total
                FROM geo_cities
                GROUP BY ars_land
            ) AS agg
            WHERE s.ars_land = agg.ars_land;
        """)


# -------------------------------------------------------
# MAIN
# -------------------------------------------------------

def main():
    print(f"Читаю файл: {XLSX_PATH}")
    df = load_raw_df()
    states_df, regions_df, districts_df, subdistricts_df, cities_df = split_levels(df)

    # --- превью ---
    print("\n=== СТАТИСТИКА (после разреза) ===")
    print(f"Земли:        {len(states_df)}")
    print(f"RB:           {len(regions_df)}")
    print(f"Kreise:       {len(districts_df)}")
    print(f"VB:           {len(subdistricts_df)}")
    print(f"Gemeinden:    {len(cities_df)}")

    print("\n=== Пример земель ===")
    print(states_df[["ars_land", "name"]].head(10).to_string(index=False))

    print("\n=== Пример Kreise ===")
    print(districts_df[["ars_land", "ars_rb", "ars_kreis", "name"]].head(10).to_string(index=False))

    print("\n=== Пример VB ===")
    if len(subdistricts_df):
        print(subdistricts_df[["ars_land", "ars_rb", "ars_kreis", "ars_vb", "name"]].head(10).to_string(index=False))
    else:
        print("(в этом файле нет satzart=50)")

    print("\n=== Пример городов ===")
    print(cities_df[["ars_land", "ars_rb", "ars_kreis", "ars_vb", "ars_gem", "name", "plz_list"]].head(10).to_string(index=False))

    # --- запись в БД ---
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE geo_cities")
                cur.execute("TRUNCATE TABLE geo_subdistricts")
                cur.execute("TRUNCATE TABLE geo_districts")
                cur.execute("TRUNCATE TABLE geo_regions")
                cur.execute("TRUNCATE TABLE geo_states")

            insert_states(states_df, conn)
            insert_regions(regions_df, conn)
            insert_districts(districts_df, conn)
            insert_subdistricts(subdistricts_df, conn)

            maps = build_maps(states_df, regions_df, districts_df, subdistricts_df)
            insert_cities(cities_df, maps, conn)

            aggregate_up(conn)

        print("\nЗапись в БД завершена.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
