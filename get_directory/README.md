# get_directory — Prototype Collector

Validation experiment: can we retrieve Austin, TX apartment listings from
OpenStreetMap and enrich a small subset with website data from Google Places?

## What it does

1. Queries the [Overpass API](https://overpass-api.de/) for nodes/ways tagged
   as residential apartment buildings inside Austin's bounding box.
2. Caps results at 50 entries.
3. Normalises each entry into readable JSON (name, lat/lon, address, website).
4. For the first 10 entries that have **no website**, it calls the
   [Google Places Find Place API](https://developers.google.com/maps/documentation/places/web-service/search-find-place)
   to attempt website resolution.
5. Saves three output files in this directory.

## Output files

| File | Contents |
|------|----------|
| `raw_osm.json` | Raw Overpass API response, truncated to 50 elements |
| `cleaned.json` | Normalised fields: name, lat, lon, address, website, phone |
| `enriched.json` | Cleaned data + `google_website` and `google_attempted` flags |

## Setup

Python 3.10+ required. No third-party packages — uses only the standard library.

```bash
# Set your Google Places API key (required for the enrichment step)
export GOOGLE_PLACES_API_KEY="your_key_here"

# Run the script
python get_directory/collect.py
```

If `GOOGLE_PLACES_API_KEY` is not set, the script still runs — it fetches and
cleans OSM data but skips the Google enrichment step.

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
