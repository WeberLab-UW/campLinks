"""
Look up candidates in the Archive of Political Emails (politicalemails.org).

Input: CSV with at minimum a name column.
Output: CSV preserving all input columns plus match metadata.

Match statuses:
  no_match  - search returned zero hits (after any filtering)
  single    - exactly one hit
  multiple  - more than one hit; org_id et al are pipe-delimited

Usage:
  python archive_lookup.py candidates.csv -o results.csv
  python archive_lookup.py candidates.csv --name-col candidate --state-col state --enrich
"""

import argparse
import csv
import logging
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from typing import Optional
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

BASE = "https://politicalemails.org"
HEADERS = {"User-Agent": "candidate-archive-lookup/0.1 (research; contact via repo)"}

log = logging.getLogger("archive_lookup")


@dataclass
class Match:
    org_id: str
    name: str
    archive_url: str
    country: Optional[str] = None
    message_count: Optional[int] = None
    state: Optional[str] = None
    party: Optional[str] = None
    office: Optional[str] = None
    website: Optional[str] = None


class ArchiveClient:
    def __init__(self, delay: float = 1.0, timeout: int = 20):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.delay = delay
        self.timeout = timeout
        self._last_call = 0.0

    def _throttle(self):
        wait = self.delay - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _get(self, url: str) -> str:
        self._throttle()
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        return r.text

    def search(self, query: str) -> list[Match]:
        url = f"{BASE}/organizations?{urlencode({'query': query})}"
        log.debug("GET %s", url)
        return parse_search_results(self._get(url))

    def profile(self, org_id: str) -> dict:
        url = f"{BASE}/organizations/{org_id}"
        log.debug("GET %s", url)
        return parse_profile(self._get(url))


def parse_search_results(html: str) -> list[Match]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[Match] = []
    for a in soup.select("a.resource-tease"):
        href = a.get("href", "")
        m = re.search(r"/organizations/(\d+)", href)
        if not m:
            continue
        org_id = m.group(1)

        name_el = a.select_one(".resource-tease__title-right")
        name = name_el.get_text(strip=True) if name_el else ""

        country = None
        flag = a.select_one(".flag-icon")
        if flag:
            for cls in flag.get("class", []):
                cm = re.match(r"flag-icon-([a-z]{2})$", cls)
                if cm:
                    country = cm.group(1)
                    break

        msg_count = None
        for meta in a.select(".resource-tease__meta-item"):
            strong = meta.find("strong")
            if strong and "message" in meta.get_text().lower():
                try:
                    msg_count = int(strong.get_text(strip=True).replace(",", ""))
                except ValueError:
                    pass
                break

        out.append(Match(
            org_id=org_id,
            name=name,
            archive_url=f"{BASE}/organizations/{org_id}",
            country=country,
            message_count=msg_count,
        ))
    return out


def parse_profile(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    out = {"state": None, "party": None, "office": None, "website": None}
    for li in soup.select("ul.key-val-list li"):
        strong = li.find("strong")
        if not strong:
            continue
        key = strong.get_text(strip=True).rstrip(":").lower()
        link = li.find("a", href=lambda h: bool(h) and "/cdn-cgi/" not in h)
        if link:
            val = link.get_text(strip=True)
        else:
            val = li.get_text(" ", strip=True)
            val = val.replace(strong.get_text(strip=True), "", 1).strip(" :")
        val = val or None
        if key == "state/locality":
            out["state"] = val
        elif key == "party":
            out["party"] = val
        elif key == "office held/sought":
            out["office"] = val
        elif key == "website":
            out["website"] = val
    return out


def enrich(client: ArchiveClient, matches: list[Match]) -> list[Match]:
    for m in matches:
        try:
            p = client.profile(m.org_id)
            m.state = p["state"]
            m.party = p["party"]
            m.office = p["office"]
            m.website = p["website"]
        except requests.RequestException as e:
            log.warning("profile fetch failed for %s: %s", m.org_id, e)
    return matches


def filter_matches(
    matches: list[Match],
    state: Optional[str] = None,
    country: Optional[str] = None,
    party: Optional[str] = None,
) -> list[Match]:
    if not (state or country or party):
        return matches
    out = []
    for m in matches:
        if country and m.country and m.country.lower() != country.lower():
            continue
        if state and m.state and m.state.strip().lower() != state.strip().lower():
            continue
        if party and m.party and party.strip().lower() not in m.party.strip().lower():
            continue
        out.append(m)
    return out


def collapse(matches: list[Match]) -> dict:
    if not matches:
        return {
            "match_status": "no_match",
            "match_count": 0,
            "org_id": "",
            "org_name": "",
            "archive_url": "",
            "message_count": "",
            "state": "",
            "party": "",
            "office": "",
            "website": "",
        }
    pipe = lambda xs: " | ".join("" if x is None else str(x) for x in xs)
    return {
        "match_status": "single" if len(matches) == 1 else "multiple",
        "match_count": len(matches),
        "org_id": pipe(m.org_id for m in matches),
        "org_name": pipe(m.name for m in matches),
        "archive_url": pipe(m.archive_url for m in matches),
        "message_count": pipe(m.message_count for m in matches),
        "state": pipe(m.state for m in matches),
        "party": pipe(m.party for m in matches),
        "office": pipe(m.office for m in matches),
        "website": pipe(m.website for m in matches),
    }


RESULT_COLS = [
    "match_status", "match_count",
    "org_id", "org_name", "archive_url", "message_count",
    "state", "party", "office", "website",
]


def lookup_one(client: ArchiveClient, name: str, *, state=None, country=None,
               party=None, enrich_profiles=False) -> dict:
    name = name.strip()
    if not name:
        return collapse([])
    try:
        results = client.search(name)
    except requests.RequestException as e:
        log.error("search failed for %r: %s", name, e)
        return {**collapse([]), "match_status": "error"}

    needs_enrich = enrich_profiles or any([state, party])
    if needs_enrich and results:
        enrich(client, results)

    filtered = filter_matches(results, state=state, country=country, party=party)
    return collapse(filtered)


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("input", help="Input CSV path")
    p.add_argument("-o", "--output", default="-",
                   help="Output CSV path (default: stdout)")
    p.add_argument("--name-col", default="name")
    p.add_argument("--state-col", default=None,
                   help="Optional input column with state name (e.g. 'Virginia')")
    p.add_argument("--country-col", default=None,
                   help="Optional input column with 2-letter country code")
    p.add_argument("--party-col", default=None,
                   help="Optional input column with party (substring match)")
    p.add_argument("--default-country", default="us",
                   help="Country code to apply when no country column given (default: us; pass empty to disable)")
    p.add_argument("--enrich", action="store_true",
                   help="Always fetch profile pages to populate state/party/office/website")
    p.add_argument("--delay", type=float, default=1.0,
                   help="Seconds between HTTP requests (default 1.0)")
    p.add_argument("--verbose", "-v", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    with open(args.input, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        in_cols = reader.fieldnames or []

    if args.name_col not in in_cols:
        sys.exit(f"name column {args.name_col!r} not found in input columns: {in_cols}")

    out_cols = in_cols + [c for c in RESULT_COLS if c not in in_cols]
    out_f = sys.stdout if args.output == "-" else open(args.output, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_f, fieldnames=out_cols)
    writer.writeheader()

    client = ArchiveClient(delay=args.delay)
    default_country = args.default_country or None

    for i, row in enumerate(rows, 1):
        name = row.get(args.name_col, "")
        state = row.get(args.state_col) if args.state_col else None
        country = row.get(args.country_col) if args.country_col else default_country
        party = row.get(args.party_col) if args.party_col else None

        log.info("[%d/%d] %s", i, len(rows), name)
        result = lookup_one(client, name, state=state, country=country,
                            party=party, enrich_profiles=args.enrich)
        writer.writerow({**row, **result})
        if out_f is not sys.stdout:
            out_f.flush()

    if out_f is not sys.stdout:
        out_f.close()


if __name__ == "__main__":
    main()
