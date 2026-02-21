# get_directory — Prototype Collector

Validation experiment: can we retrieve Austin, TX apartment listings from
OpenStreetMap and enrich a small subset with website data from Google Places?

## What it does

1. Queries the [Overpass API](https://overpass-api.de/) for nodes/ways tagged
   as residential apartment buildings inside Austin's bounding box.
2. Caps results at 50 entries.
3. Normalises each entry into readable JSON (name, lat/lon, address, website).
4. For the first 10 entries that have **no website**, it performs a two-step
   Google Places enrichment flow using Places API (New):
   - Find Place: resolve `place_id` from apartment name + `Austin TX` and/or address
     (with location bias near the entry lat/lon).
   - Place Details: fetch only the `website` field for that `place_id`.
5. Saves three output files in this directory.

## Output files

| File | Contents |
|------|----------|
| `raw_osm.json` | Raw Overpass API response, truncated to 50 elements |
| `cleaned.json` | Normalised fields: name, lat, lon, address, website, phone |
| `enriched.json` | Cleaned data + enrichment results (`google_website`, `google_attempted`) and debug fields (`google_query`, `google_query_used`, `google_candidates_returned`, `google_candidates_count`, `google_place_id`, `google_find_status`, `google_find_error_message`, `google_details_status`, `google_details_had_website`, `google_status`) |

## Setup

Python 3.10+ required. Only one third-party dependency: `python-dotenv`.

```bash
pip install python-dotenv
```

Create a `.env` file inside `get_directory/` from the provided example:

```bash
cp get_directory/.env.example get_directory/.env
# then edit .env and paste your key
```

```ini
# get_directory/.env
GOOGLE_PLACES_API_KEY=your_google_places_api_key_here
```

Run the script:

```bash
python get_directory/collect.py
```

The `.env` file is git-ignored. If `GOOGLE_PLACES_API_KEY` is absent (or the
`.env` file doesn't exist), the script still runs — it fetches and cleans OSM
data but skips the Google enrichment step.

## Canonicalize Homepages

After `enriched.json` is generated, run:

```bash
python3 get_directory/canonicalize_homepages.py
```

This creates:

- `homepages_canonicalized.json`

For each discovered website, it saves:

- `homepage_raw` (original URL from enriched output)
- `homepage_final` (URL after stripping tracking params and following redirects)

## Console output

The script logs progress clearly:

```
=== Austin Apartment Directory — Prototype Collector ===

[OSM] Sending Overpass query...
[OSM] Response received — 38 element(s) returned.
[Save] Written: .../raw_osm.json
[Clean] 38 entries after normalization.
[Save] Written: .../cleaned.json
[Google] Will attempt to resolve 10 entries (limit: 10).
  [1/10] Looking up: Arbor at Quail Creek
    -> Found: https://www.example-complex.com/
  ...
[Google] Resolved 6/10 websites.
[Save] Written: .../enriched.json

==================================================
  SUMMARY
==================================================
  Total entries retrieved (OSM):     38
  Already had website (from OSM):    5
  Missing website (needed lookup):   33
  Sent to Google Places:             10
  Resolved by Google:                6
  Still no website:                  27
==================================================
```

## This is NOT

- A production pipeline
- A database-backed system
- A full city scrape
- Optimised for performance

It is a small experiment to validate the data retrieval strategy before
building anything larger.
