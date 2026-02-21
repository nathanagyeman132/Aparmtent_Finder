"""
Canonicalize apartment homepage URLs from enriched output.

Usage:
    python3 get_directory/canonicalize_homepages.py
    python3 get_directory/canonicalize_homepages.py --input get_directory/enriched.json --output get_directory/homepages.json
"""

import argparse
import json
import os
import urllib.parse
import urllib.request
import urllib.error


DEFAULT_INPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "enriched.json")
DEFAULT_OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "homepages_canonicalized.json")

TRACKING_KEYS = {
    "gclid",
    "fbclid",
    "msclkid",
    "mc_cid",
    "mc_eid",
    "switch_cls[id]",
    "rcstdid",
    "ilm",
}


def strip_tracking_params(url: str) -> str:
    """Remove known tracking query params and utm_* params from a URL."""
    parsed = urllib.parse.urlsplit(url)
    pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    filtered = []
    for key, value in pairs:
        key_l = key.lower()
        if key_l.startswith("utm_") or key_l in TRACKING_KEYS:
            continue
        filtered.append((key, value))

    clean_query = urllib.parse.urlencode(filtered, doseq=True)
    return urllib.parse.urlunsplit((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        clean_query,
        "",  # strip fragment
    ))


def ensure_scheme(url: str) -> str:
    """Default to https when URL has no scheme."""
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme:
        return url
    return f"https://{url.lstrip('/')}"


def host_variants(url: str) -> list[str]:
    """
    Build fallback variants:
    - toggle scheme https/http
    - toggle www/non-www
    """
    parsed = urllib.parse.urlsplit(url)
    host = parsed.netloc
    variants: list[str] = []

    def build(scheme: str, netloc: str) -> str:
        return urllib.parse.urlunsplit((scheme, netloc, parsed.path, parsed.query, ""))

    schemes = [parsed.scheme] if parsed.scheme else ["https", "http"]
    if "https" not in schemes:
        schemes.append("https")
    if "http" not in schemes:
        schemes.append("http")

    hosts = [host]
    if host.startswith("www."):
        hosts.append(host[4:])
    else:
        hosts.append(f"www.{host}")

    seen: set[str] = set()
    for scheme in schemes:
        for h in hosts:
            candidate = build(scheme, h)
            if candidate not in seen:
                seen.add(candidate)
                variants.append(candidate)
    return variants


def try_fetch_once(url: str, timeout: int = 20) -> tuple[str | None, bool]:
    """
    Return (best_url, hard_success).
    hard_success=True only when HTTP request completes without HTTPError.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.geturl(), True
    except urllib.error.HTTPError as exc:
        # Even on 4xx/5xx, Google-style/bot-protected sites often redirect first.
        # Keep the best-known final URL if available.
        return exc.geturl() or url, False
    except (urllib.error.URLError, ValueError):
        return None, False


def resolve_final_url(seed_url: str, timeout: int = 20) -> str | None:
    """
    Load URL once and return final URL after redirects.
    Returns None on network/HTTP failures.
    """
    canonical_seed = ensure_scheme(seed_url)
    candidates = host_variants(canonical_seed)

    first_soft_url: str | None = None
    for candidate in candidates:
        final_url, hard_success = try_fetch_once(candidate, timeout=timeout)
        if hard_success and final_url:
            return final_url
        if final_url and not first_soft_url:
            first_soft_url = final_url

    # Fallback: if nothing succeeded, keep best-known URL (or canonical seed).
    return first_soft_url or canonical_seed


def extract_seed_urls(enriched: list[dict]) -> list[str]:
    """Pull website candidates from enriched entries, de-duplicated."""
    seen: set[str] = set()
    urls: list[str] = []

    for entry in enriched:
        seed = entry.get("website") or entry.get("google_website")
        if seed and seed not in seen:
            seen.add(seed)
            urls.append(seed)

    return urls


def canonicalize(enriched_path: str, output_path: str) -> None:
    with open(enriched_path, "r", encoding="utf-8") as fh:
        enriched = json.load(fh)

    seed_urls = extract_seed_urls(enriched)
    output_rows = []

    print(f"[Canonicalize] Processing {len(seed_urls)} website(s) from {enriched_path}")
    for idx, raw in enumerate(seed_urls, start=1):
        canonical_seed = strip_tracking_params(raw)
        final = resolve_final_url(canonical_seed)
        output_rows.append({
            "homepage_raw": raw,
            "homepage_final": final,
        })
        print(f"  [{idx}/{len(seed_urls)}] {canonical_seed} -> {final or 'null'}")

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(output_rows, fh, indent=2, ensure_ascii=False)

    print(f"[Save] Written: {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Canonicalize apartment homepage URLs.")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Path to enriched.json")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Path to output JSON")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    canonicalize(args.input, args.output)


if __name__ == "__main__":
    main()
