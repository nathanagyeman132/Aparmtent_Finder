"""
get_directory/collect.py

Prototype: Retrieve Austin, TX apartment complexes from OpenStreetMap (Overpass API)
and enrich a small subset with website data via Google Places API.

This is a validation experiment — not a production system.

Usage:
    python collect.py

Environment variables (loaded from get_directory/.env if present):
    GOOGLE_PLACES_API_KEY  — required for the Google enrichment step
"""

import json
import os
import time
import urllib.request
import urllib.parse
import urllib.error

from dotenv import load_dotenv

# Load .env from the same directory as this script
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
GOOGLE_PLACES_URL = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"

OSM_RESULT_LIMIT = 50          # cap how many nodes/ways we keep from OSM
GOOGLE_ENRICH_LIMIT = 10       # max entries sent to Google Places

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_OSM_FILE = os.path.join(OUTPUT_DIR, "raw_osm.json")
CLEANED_FILE = os.path.join(OUTPUT_DIR, "cleaned.json")
ENRICHED_FILE = os.path.join(OUTPUT_DIR, "enriched.json")


# ---------------------------------------------------------------------------
# Step 1 — Fetch from Overpass API
# ---------------------------------------------------------------------------

def fetch_osm_apartments() -> dict:
    """
    Query Overpass API for nodes/ways tagged as residential apartments in Austin.
    Returns the raw JSON response (dict).
    """
    # Bounding box roughly covers Austin city limits
    # [south, west, north, east]
    bbox = "30.1,-97.95,30.5,-97.55"

    query = f"""
[out:json][timeout:30];
(
  node["building"="apartments"]({bbox});
  way["building"="apartments"]({bbox});
  node["residential"="apartments"]({bbox});
  way["residential"="apartments"]({bbox});
);
out center {OSM_RESULT_LIMIT};
"""

    data = urllib.parse.urlencode({"data": query}).encode()
    req = urllib.request.Request(OVERPASS_URL, data=data)
    req.add_header("User-Agent", "ApartmentFinderPrototype/0.1")

    print("[OSM] Sending Overpass query...")
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = json.loads(resp.read().decode())

    print(f"[OSM] Response received — {len(raw.get('elements', []))} element(s) returned.")
    return raw


# ---------------------------------------------------------------------------
# Step 2 — Clean / normalize OSM elements
# ---------------------------------------------------------------------------

def clean_element(el: dict) -> dict:
    """Extract readable fields from a single OSM element."""
    tags = el.get("tags", {})

    # Latitude/longitude: nodes have direct lat/lon; ways use the computed centre
    if el["type"] == "node":
        lat = el.get("lat")
        lon = el.get("lon")
    else:
        centre = el.get("center", {})
        lat = centre.get("lat")
        lon = centre.get("lon")

    address_parts = [
        tags.get("addr:housenumber", ""),
        tags.get("addr:street", ""),
        tags.get("addr:city", ""),
        tags.get("addr:state", ""),
        tags.get("addr:postcode", ""),
    ]
    address = " ".join(p for p in address_parts if p).strip() or None

    return {
        "osm_id": el.get("id"),
        "osm_type": el.get("type"),
        "name": tags.get("name") or tags.get("addr:housename") or None,
        "lat": lat,
        "lon": lon,
        "address": address,
        "website": tags.get("website") or tags.get("url") or None,
        "phone": tags.get("phone") or tags.get("contact:phone") or None,
    }


def clean_osm_data(raw: dict) -> list[dict]:
    elements = raw.get("elements", [])
    cleaned = [clean_element(el) for el in elements]
    print(f"[Clean] {len(cleaned)} entries after normalization.")
    return cleaned


# ---------------------------------------------------------------------------
# Step 3 — Enrich via Google Places API
# ---------------------------------------------------------------------------

def google_find_website(entry: dict, api_key: str) -> str | None:
    """
    Use Google Places 'Find Place' to look up the official website for one entry.
    Returns the website URL string, or None if not found.
    """
    name = entry.get("name") or ""
    address = entry.get("address") or ""

    if name:
        query = f"{name} apartments Austin TX"
    elif address:
        query = f"apartments {address} Austin TX"
    else:
        return None

    params = urllib.parse.urlencode({
        "input": query,
        "inputtype": "textquery",
        "fields": "name,website",
        "locationbias": "circle:20000@30.2672,-97.7431",  # Austin city center
        "key": api_key,
    })

    url = f"{GOOGLE_PLACES_URL}?{params}"
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "ApartmentFinderPrototype/0.1")

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read().decode())

        candidates = result.get("candidates", [])
        if candidates:
            return candidates[0].get("website") or None
    except urllib.error.URLError as exc:
        print(f"    [Google] Network error for '{query}': {exc}")

    return None


def enrich_with_google(cleaned: list[dict]) -> list[dict]:
    api_key = os.environ.get("GOOGLE_PLACES_API_KEY", "")
    if not api_key:
        print("[Google] GOOGLE_PLACES_API_KEY not set — skipping enrichment step.")
        return [dict(entry, google_website=None, google_attempted=False) for entry in cleaned]

    needs_website = [e for e in cleaned if not e.get("website")]
    to_enrich = needs_website[:GOOGLE_ENRICH_LIMIT]

    print(f"[Google] Will attempt to resolve {len(to_enrich)} entries (limit: {GOOGLE_ENRICH_LIMIT}).")

    enrichment_map: dict[int, str | None] = {}
    resolved = 0

    for i, entry in enumerate(to_enrich, start=1):
        label = entry.get("name") or entry.get("address") or f"osm:{entry['osm_id']}"
        print(f"  [{i}/{len(to_enrich)}] Looking up: {label}")
        website = google_find_website(entry, api_key)
        enrichment_map[entry["osm_id"]] = website
        if website:
            resolved += 1
            print(f"    -> Found: {website}")
        else:
            print(f"    -> Not found.")
        time.sleep(0.25)   # gentle rate limiting

    print(f"[Google] Resolved {resolved}/{len(to_enrich)} websites.")

    enriched = []
    for entry in cleaned:
        eid = entry["osm_id"]
        if eid in enrichment_map:
            enriched.append(dict(entry, google_website=enrichment_map[eid], google_attempted=True))
        else:
            enriched.append(dict(entry, google_website=None, google_attempted=False))

    return enriched


# ---------------------------------------------------------------------------
# Step 4 — Save output files
# ---------------------------------------------------------------------------

def save_json(path: str, data) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    print(f"[Save] Written: {path}")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(cleaned: list[dict], enriched: list[dict]) -> None:
    total = len(cleaned)
    had_osm_website = sum(1 for e in cleaned if e.get("website"))
    needed_google = sum(1 for e in enriched if e.get("google_attempted"))
    google_resolved = sum(1 for e in enriched if e.get("google_website"))

    print()
    print("=" * 50)
    print("  SUMMARY")
    print("=" * 50)
    print(f"  Total entries retrieved (OSM):     {total}")
    print(f"  Already had website (from OSM):    {had_osm_website}")
    print(f"  Missing website (needed lookup):   {total - had_osm_website}")
    print(f"  Sent to Google Places:             {needed_google}")
    print(f"  Resolved by Google:                {google_resolved}")
    print(f"  Still no website:                  {total - had_osm_website - google_resolved}")
    print("=" * 50)
    print()
    print(f"Output files written to: {OUTPUT_DIR}/")
    print(f"  raw_osm.json   — raw Overpass response (capped at {OSM_RESULT_LIMIT} elements)")
    print(f"  cleaned.json   — normalized fields")
    print(f"  enriched.json  — cleaned + Google website enrichment")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print()
    print("=== Austin Apartment Directory — Prototype Collector ===")
    print()

    # 1. Fetch raw OSM data
    raw = fetch_osm_apartments()

    # Trim elements to the limit before saving so raw file stays small
    if len(raw.get("elements", [])) > OSM_RESULT_LIMIT:
        raw["elements"] = raw["elements"][:OSM_RESULT_LIMIT]
        raw["_truncated_to"] = OSM_RESULT_LIMIT

    save_json(RAW_OSM_FILE, raw)

    # 2. Clean
    cleaned = clean_osm_data(raw)
    save_json(CLEANED_FILE, cleaned)

    # 3. Enrich with Google
    enriched = enrich_with_google(cleaned)
    save_json(ENRICHED_FILE, enriched)

    # 4. Summary
    print_summary(cleaned, enriched)


if __name__ == "__main__":
    main()
