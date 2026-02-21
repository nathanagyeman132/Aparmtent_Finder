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
load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
GOOGLE_TEXT_SEARCH_NEW_URL = "https://places.googleapis.com/v1/places:searchText"
GOOGLE_PLACE_DETAILS_NEW_URL = "https://places.googleapis.com/v1/places"

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

def build_google_query(entry: dict) -> str | None:
    """Build the text query for Google Find Place."""
    name = (entry.get("name") or "").strip()
    address = (entry.get("address") or "").strip()

    if name and address:
        return f"{name} Austin TX {address}"
    if name:
        return f"{name} Austin TX"
    if address:
        return f"{address} Austin TX apartments"
    return None


def build_location_bias(entry: dict) -> dict:
    """
    Bias Find Place near the apartment's coordinates when available.
    Falls back to Austin city center.
    """
    lat = entry.get("lat")
    lon = entry.get("lon")
    if lat is None or lon is None:
        lat = 30.2672
        lon = -97.7431
        radius = 20000.0
    else:
        radius = 5000.0
    return {
        "circle": {
            "center": {
                "latitude": lat,
                "longitude": lon,
            },
            "radius": radius,
        }
    }


def google_find_place_id(entry: dict, api_key: str) -> tuple[str | None, dict]:
    """
    Step 1: Resolve place_id via Find Place.
    Returns: (place_id | None, debug_info)
    """
    query = build_google_query(entry)
    debug = {
        "google_query": query,
        "google_query_used": query,
        "google_candidates_returned": False,
        "google_candidates_count": 0,
        "google_place_id": None,
        "google_find_status": None,
        "google_find_error_message": None,
    }

    if not query:
        return None, debug

    params = urllib.parse.urlencode({
        "fields": "places.id",
        "key": api_key,
    })
    url = f"{GOOGLE_TEXT_SEARCH_NEW_URL}?{params}"

    # Try with location bias first; if Google rejects payload as INVALID_ARGUMENT,
    # retry once without bias to isolate malformed-bias failures.
    payload_candidates = [
        {"textQuery": query, "locationBias": build_location_bias(entry)},
        {"textQuery": query},
    ]

    for payload_obj in payload_candidates:
        payload = json.dumps(payload_obj).encode("utf-8")
        req = urllib.request.Request(url, data=payload, method="POST")
        req.add_header("User-Agent", "ApartmentFinderPrototype/0.1")
        req.add_header("Content-Type", "application/json")

        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read().decode())

            places = result.get("places", [])
            debug["google_find_status"] = "OK" if places else "ZERO_RESULTS"
            debug["google_candidates_count"] = len(places)
            debug["google_candidates_returned"] = bool(places)
            if places:
                place_id = places[0].get("id")
                debug["google_place_id"] = place_id
                return place_id, debug
            return None, debug
        except urllib.error.HTTPError as exc:
            debug["google_find_status"] = "HTTP_ERROR"
            retryable_invalid_argument = False
            try:
                err_body = json.loads(exc.read().decode())
                error_obj = err_body.get("error", {})
                err_status = error_obj.get("status")
                err_message = error_obj.get("message")
                if err_status:
                    debug["google_find_status"] = err_status
                debug["google_find_error_message"] = err_message
                retryable_invalid_argument = (
                    err_status == "INVALID_ARGUMENT" and "locationBias" in payload_obj
                )
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
            print(f"    [Google] HTTP error for '{query}': {exc}")
            if retryable_invalid_argument:
                continue
            return None, debug
        except urllib.error.URLError as exc:
            debug["google_find_status"] = "NETWORK_ERROR"
            debug["google_find_error_message"] = str(exc)
            print(f"    [Google] Network error for '{query}': {exc}")
            return None, debug

    return None, debug


def google_get_place_website(place_id: str, api_key: str) -> tuple[str | None, str | None]:
    """
    Step 2: Fetch website from Place Details (New) using place_id.
    """
    place_id_escaped = urllib.parse.quote(place_id, safe="")
    params = urllib.parse.urlencode({
        "fields": "websiteUri",
        "key": api_key,
    })
    url = f"{GOOGLE_PLACE_DETAILS_NEW_URL}/{place_id_escaped}?{params}"
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "ApartmentFinderPrototype/0.1")

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read().decode())
        website = result.get("websiteUri") or None
        return website, "OK"
    except urllib.error.HTTPError as exc:
        details_status = "HTTP_ERROR"
        try:
            err_body = json.loads(exc.read().decode())
            err_status = err_body.get("error", {}).get("status")
            if err_status:
                details_status = err_status
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass
        print(f"    [Google] HTTP error for place_id '{place_id}': {exc}")
        return None, details_status
    except urllib.error.URLError as exc:
        print(f"    [Google] Network error for place_id '{place_id}': {exc}")
        return None, "NETWORK_ERROR"


def enrich_with_google(cleaned: list[dict]) -> list[dict]:
    api_key = os.environ.get("GOOGLE_PLACES_API_KEY", "")
    if not api_key:
        print("[Google] GOOGLE_PLACES_API_KEY not set — skipping enrichment step.")
        return [
            dict(
                entry,
                google_website=None,
                google_attempted=False,
                google_query=None,
                google_query_used=None,
                google_candidates_returned=False,
                google_candidates_count=0,
                google_place_id=None,
                google_find_status=None,
                google_find_error_message=None,
                google_details_status=None,
                google_details_had_website=False,
                google_status=None,
            )
            for entry in cleaned
        ]

    needs_website = [e for e in cleaned if not e.get("website")]
    needs_website.sort(key=lambda e: 0 if e.get("address") else 1)
    to_enrich = needs_website[:GOOGLE_ENRICH_LIMIT]

    print(f"[Google] Will attempt to resolve {len(to_enrich)} entries (limit: {GOOGLE_ENRICH_LIMIT}).")

    enrichment_map: dict[int, dict] = {}
    resolved = 0

    for i, entry in enumerate(to_enrich, start=1):
        label = entry.get("name") or entry.get("address") or f"osm:{entry['osm_id']}"
        print(f"  [{i}/{len(to_enrich)}] Looking up: {label}")
        place_id, debug = google_find_place_id(entry, api_key)
        if place_id:
            website, details_status = google_get_place_website(place_id, api_key)
        else:
            website, details_status = None, "SKIPPED_NO_PLACE_ID"
        debug["google_details_status"] = details_status
        debug["google_details_had_website"] = bool(website)
        if debug.get("google_find_status") != "OK":
            debug["google_status"] = debug.get("google_find_status")
        elif details_status and details_status != "OK":
            debug["google_status"] = details_status
        else:
            debug["google_status"] = "OK"
        debug["google_website"] = website
        enrichment_map[entry["osm_id"]] = debug
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
            enriched.append(dict(entry, google_attempted=True, **enrichment_map[eid]))
        else:
            enriched.append(
                dict(
                    entry,
                    google_website=None,
                    google_attempted=False,
                    google_query=None,
                    google_query_used=None,
                    google_candidates_returned=False,
                    google_candidates_count=0,
                    google_place_id=None,
                    google_find_status=None,
                    google_find_error_message=None,
                    google_details_status=None,
                    google_details_had_website=False,
                    google_status=None,
                )
            )

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
