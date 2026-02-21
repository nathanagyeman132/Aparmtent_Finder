"""
loader.py

Reads the three JSON output files from get_directory.py and upserts them
into the Postgres data lake running in Docker.

Usage:
    python loader.py

Environment variables (optional overrides):
    POSTGRES_HOST      default: localhost
    POSTGRES_PORT      default: 5432
    POSTGRES_DB        default: aptdb
    POSTGRES_USER      default: apt
    POSTGRES_PASSWORD  default: aptpass
"""

import json
import os
import sys

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# loader.py lives in Docker_Test/; .env is in the same folder,
# JSON outputs are one level up (project root)
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))

load_dotenv()

RAW_OSM_FILE = os.path.join(ROOT, "raw_osm.json")
CLEANED_FILE = os.path.join(ROOT, "cleaned.json")
ENRICHED_FILE = os.path.join(ROOT, "enriched.json")


def get_connection():
    print("DEBUG DB CONFIG:")
    print("HOST:", os.environ.get("POSTGRES_HOST", "localhost"))
    print("USER:", os.environ.get("POSTGRES_USER", "apt"))
    print("DB:", os.environ.get("POSTGRES_DB", "aptdb"))
    print("PASSWORD:", os.environ.get("POSTGRES_PASSWORD", "root"))
    return psycopg2.connect(
        # host="localhost",
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ.get("POSTGRES_DB", "aptdb"),
        user=os.environ.get("POSTGRES_USER", "apt"),
        password=os.environ.get("POSTGRES_PASSWORD", "root"),
    )


def load_json(path: str):
    if not os.path.exists(path):
        print(f"[WARN] File not found, skipping: {path}")
        return None
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def upsert_apartments(cur, records: list[dict]) -> int:
    rows = [
        (
            r["osm_id"],
            r.get("osm_type"),
            r.get("name"),
            r.get("lat"),
            r.get("lon"),
            r.get("address"),
            r.get("website"),
            r.get("phone"),
        )
        for r in records
    ]
    execute_values(
        cur,
        """
        INSERT INTO apartments (osm_id, osm_type, name, lat, lon, address, website, phone)
        VALUES %s
        ON CONFLICT (osm_id) DO UPDATE SET
            osm_type    = EXCLUDED.osm_type,
            name        = EXCLUDED.name,
            lat         = EXCLUDED.lat,
            lon         = EXCLUDED.lon,
            address     = EXCLUDED.address,
            website     = EXCLUDED.website,
            phone       = EXCLUDED.phone,
            ingested_at = NOW()
        """,
        rows,
    )
    return len(rows)


def upsert_enrichment(cur, records: list[dict]) -> int:
    rows = [
        (
            r["osm_id"],
            r.get("google_attempted"),
            r.get("google_query"),
            r.get("google_place_id"),
            r.get("google_candidates_returned"),
            r.get("google_candidates_count"),
            r.get("google_find_status"),
            r.get("google_find_error_message"),
            r.get("google_details_status"),
            r.get("google_details_had_website"),
            r.get("google_status"),
            r.get("google_website"),
        )
        for r in records
    ]
    execute_values(
        cur,
        """
        INSERT INTO apartments_enrichment (
            osm_id, google_attempted, google_query, google_place_id,
            google_candidates_returned, google_candidates_count,
            google_find_status, google_find_error_message,
            google_details_status, google_details_had_website,
            google_status, google_website
        )
        VALUES %s
        ON CONFLICT (osm_id) DO UPDATE SET
            google_attempted           = EXCLUDED.google_attempted,
            google_query               = EXCLUDED.google_query,
            google_place_id            = EXCLUDED.google_place_id,
            google_candidates_returned = EXCLUDED.google_candidates_returned,
            google_candidates_count    = EXCLUDED.google_candidates_count,
            google_find_status         = EXCLUDED.google_find_status,
            google_find_error_message  = EXCLUDED.google_find_error_message,
            google_details_status      = EXCLUDED.google_details_status,
            google_details_had_website = EXCLUDED.google_details_had_website,
            google_status              = EXCLUDED.google_status,
            google_website             = EXCLUDED.google_website,
            enriched_at                = NOW()
        """,
        rows,
    )
    return len(rows)


def insert_raw_osm(cur, payload: dict) -> None:
    cur.execute(
        "INSERT INTO raw_osm (payload) VALUES (%s)",
        (json.dumps(payload),),
    )


def main():
    print()
    print("=== Apartment Data Lake Loader ===")
    print()

    cleaned = load_json(CLEANED_FILE)
    enriched = load_json(ENRICHED_FILE)
    raw = load_json(RAW_OSM_FILE)

    if not cleaned and not enriched and not raw:
        print("[ERROR] No input files found. Run get_directory.py first.")
        sys.exit(1)

    print("[DB] Connecting to Postgres...")
    try:
        conn = get_connection()
    except Exception as exc:
        print(f"[ERROR] Could not connect: {exc}")
        print("       Is the Docker stack running? (docker compose up -d)")
        sys.exit(1)

    with conn:
        with conn.cursor() as cur:
            if cleaned:
                n = upsert_apartments(cur, cleaned)
                print(f"[apartments]            {n} rows upserted")

            if enriched:
                n = upsert_enrichment(cur, enriched)
                print(f"[apartments_enrichment] {n} rows upserted")

            if raw:
                insert_raw_osm(cur, raw)
                print(f"[raw_osm]               1 payload inserted")

    conn.close()
    print()
    print("Done.")


if __name__ == "__main__":
    main()
