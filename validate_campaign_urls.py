"""
validate_campaign_urls.py

Replace aggregator URLs with real campaign sites using the Brave Search API,
falling back to Wayback Machine snapshots for dead or historical campaigns.

Usage:
    export BRAVE_API_KEY=...
    python validate_campaign_urls.py \\
        --input  bad_aggregator_urls.csv \\
        --output validated.csv \\
        --concurrency 4 \\
        --limit 50          # try a small sample first
        --resume            # skip rows already in output
        --archive-live      # also snapshot live URLs to Wayback

Output columns:
    candidate_id, candidate_name, state, year, bad_url,
    found_url, confidence, signal_count, status_code, wayback_url, verdict

verdict values:
    live_campaign      live URL with >=2 campaign signals (donate, FEC disclaimer, etc)
    wayback_recovered  candidate URL was dead, Wayback returned a snapshot
    dead_candidate     candidate URL was dead, no Wayback snapshot found
    no_candidate       Brave returned nothing usable above the score threshold
"""

import argparse
import asyncio
import csv
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx


DENYLIST = {
    "ourcampaigns.com", "vote-usa.org", "stage.vote-usa.org",
    "bluevoterguide.org", "lykelect.com",
    "votesmart.org", "justfacts.votesmart.org",
    "ivoterguide.com",
    "uselectionatlas.org", "mail.uselectionatlas.org",
    "electiondatabase.nhpr.org", "electjon.com",
    "onyourballot.vote411.org", "vote411.org",
    "dos.elections.myflorida.com",
    "vote.org", "votesafe.org",
    "electionfraud.heritage.org", "ballotready.org",
    "politics1.com", "followthemoney.org",
    "ballotpedia.org", "opensecrets.org", "fec.gov",
    "wikipedia.org", "en.wikipedia.org",
    "linkedin.com",
    "facebook.com", "m.facebook.com",
    "twitter.com", "x.com",
    "instagram.com", "youtube.com", "tiktok.com",
    "smartvoter.org", "google.com",
}

GOV_HOST_RE = re.compile(r"(^|\.)gov$")

CAMPAIGN_SIGNALS = [
    re.compile(r"\bdonate\b", re.I),
    re.compile(r"\bvolunteer\b", re.I),
    re.compile(r"actblue\.com", re.I),
    re.compile(r"winred\.com", re.I),
    re.compile(r"anedot\.com", re.I),
    re.compile(r"paid for by", re.I),
    re.compile(r"\bendorsements?\b", re.I),
    re.compile(r"join (the |our )?campaign", re.I),
    re.compile(r"\byard sign\b", re.I),
    re.compile(r"\b(meet|about) (the )?candidate\b", re.I),
    re.compile(r"\bissues?\b.*\b(platform|priorities)\b", re.I),
]

CAMPAIGN_DOMAIN_KEYWORDS = (
    "forsenate", "forcongress", "forhouse", "forgovernor", "formayor",
    "forcouncil", "forassembly", "fordelegate", "forstaterep",
    "vote", "elect", "campaign",
)

BRAVE_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"
WAYBACK_AVAILABLE = "https://archive.org/wayback/available"

USER_AGENT = "Mozilla/5.0 (compatible; CampaignValidator/1.0)"


def domain_of(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return ""
    return host[4:] if host.startswith("www.") else host


def is_denied(url: str) -> bool:
    host = domain_of(url)
    if not host:
        return True
    if host in DENYLIST:
        return True
    if any(host.endswith("." + d) for d in DENYLIST):
        return True
    if GOV_HOST_RE.search(host):
        return True
    return False


def score_url(url: str, candidate_name: str) -> float:
    score = 0.0
    parsed = urlparse(url)
    host = parsed.netloc.lower().removeprefix("www.")
    path = parsed.path.lower()

    name_parts = re.findall(r"[a-z]+", candidate_name.lower())
    last = name_parts[-1] if name_parts else ""
    first = name_parts[0] if name_parts else ""
    host_flat = host.replace("-", "").replace(".", "")
    if last and last in host_flat:
        score += 0.5
    if first and first in host_flat:
        score += 0.2
    if any(kw in host_flat for kw in CAMPAIGN_DOMAIN_KEYWORDS):
        score += 0.2
    if re.search(r"20(2[3-9])", host):
        score += 0.1
    if path in ("", "/"):
        score += 0.1
    if any(p in path for p in ("/candidate", "/biography", "/results", "/profile", "/people/")):
        score -= 0.3
    return max(0.0, min(1.0, score))


def has_campaign_signals(html: str) -> int:
    return sum(1 for p in CAMPAIGN_SIGNALS if p.search(html))


def election_timestamp(year: str) -> str:
    try:
        y = int(year)
    except (TypeError, ValueError):
        y = datetime.now().year
    return f"{y}1101"


def build_query(name: str, state: str, race: str, year: str) -> str:
    negatives = " ".join(
        f"-site:{d}"
        for d in (
            "ballotpedia.org", "votesmart.org", "ourcampaigns.com",
            "vote-usa.org", "bluevoterguide.org", "ivoterguide.com",
            "lykelect.com", "uselectionatlas.org", "vote.org",
            "ballotready.org", "vote411.org", "wikipedia.org",
            "facebook.com", "linkedin.com", "twitter.com", "x.com",
        )
    )
    office = (race or "").lower()
    return f'"{name}" {state} {year} {office} campaign {negatives}'


@dataclass
class Result:
    candidate_id: str
    candidate_name: str
    state: str
    year: str
    bad_url: str
    found_url: str
    confidence: float
    signal_count: int
    status_code: int
    wayback_url: str
    verdict: str


_BRAVE_ERR_LOGGED = 0
_BRAVE_ERR_LIMIT = 5


def _log_brave_error(msg: str) -> None:
    global _BRAVE_ERR_LOGGED
    if _BRAVE_ERR_LOGGED < _BRAVE_ERR_LIMIT:
        print(f"[brave] {msg}", file=sys.stderr)
        _BRAVE_ERR_LOGGED += 1
        if _BRAVE_ERR_LOGGED == _BRAVE_ERR_LIMIT:
            print("[brave] (further errors suppressed)", file=sys.stderr)


async def brave_search(client, query, api_key):
    headers = {"Accept": "application/json", "X-Subscription-Token": api_key}
    params = {"q": query, "count": 10, "safesearch": "off"}
    last_err = ""
    for attempt in range(3):
        try:
            r = await client.get(BRAVE_ENDPOINT, headers=headers, params=params, timeout=20)
            if r.status_code == 429:
                last_err = f"429 (attempt {attempt + 1}); body: {r.text[:200]}"
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            if r.status_code >= 400:
                _log_brave_error(f"HTTP {r.status_code}: {r.text[:300]}")
                return []
            return r.json().get("web", {}).get("results", []) or []
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            await asyncio.sleep(1.0 * (attempt + 1))
    if last_err:
        _log_brave_error(last_err)
    return []


async def fetch_page(client, url):
    try:
        r = await client.get(
            url, timeout=15, follow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        )
        return r.status_code, (r.text if r.status_code == 200 else "")
    except Exception:
        return 0, ""


async def wayback_lookup(client, url, timestamp):
    try:
        r = await client.get(
            WAYBACK_AVAILABLE,
            params={"url": url, "timestamp": timestamp},
            timeout=15,
        )
        r.raise_for_status()
        snap = r.json().get("archived_snapshots", {}).get("closest", {})
        if snap.get("available"):
            return snap.get("url", "")
    except Exception:
        pass
    return ""


async def process_candidate(client, row, api_key, archive_live):
    name = row.get("candidate_name", "")
    state = row.get("state", "")
    year = row.get("year", "")
    race = row.get("race_type", "")
    bad_url = row.get("bad_campaign_url", "")

    query = build_query(name, state, race, year)
    results = await brave_search(client, query, api_key)

    scored = []
    for r in results:
        u = r.get("url", "")
        if not u or is_denied(u):
            continue
        scored.append((score_url(u, name), u))
    scored.sort(reverse=True)

    found_url, confidence, signals, status_code, verdict = "", 0.0, 0, 0, "no_candidate"

    for score, url in scored[:3]:
        if score < 0.3:
            break
        st, html = await fetch_page(client, url)
        sigs = has_campaign_signals(html) if html else 0
        if st == 200 and sigs >= 2:
            found_url, confidence, signals, status_code = url, score, sigs, st
            verdict = "live_campaign"
            break
        if st in (0, 404, 410, 500, 502, 503) and not found_url:
            found_url, confidence, status_code = url, score, st
            verdict = "dead_candidate"

    wayback = ""
    if verdict == "dead_candidate" or (archive_live and verdict == "live_campaign"):
        wayback = await wayback_lookup(client, found_url, election_timestamp(year))
        if wayback and verdict == "dead_candidate":
            verdict = "wayback_recovered"

    return Result(
        candidate_id=row.get("candidate_id", ""),
        candidate_name=name,
        state=state,
        year=year,
        bad_url=bad_url,
        found_url=found_url,
        confidence=round(confidence, 2),
        signal_count=signals,
        status_code=status_code,
        wayback_url=wayback,
        verdict=verdict,
    )


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--archive-live", action="store_true",
                    help="Also fetch a Wayback snapshot for live URLs")
    args = ap.parse_args()

    api_key = os.environ.get("BRAVE_API_KEY")
    if not api_key:
        sys.exit("BRAVE_API_KEY env var is required")

    with open(args.input, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if args.limit:
        rows = rows[: args.limit]

    out_path = Path(args.output)
    done_ids = set()
    if args.resume and out_path.exists():
        with open(out_path, newline="", encoding="utf-8") as f:
            done_ids = {r["candidate_id"] for r in csv.DictReader(f)}
    rows = [r for r in rows if r.get("candidate_id") not in done_ids]
    print(f"Processing {len(rows)} candidates ({len(done_ids)} already done)")

    fieldnames = list(Result.__dataclass_fields__.keys())
    write_header = not (args.resume and out_path.exists())
    out_f = open(out_path, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_f, fieldnames=fieldnames)
    if write_header:
        writer.writeheader()

    sem = asyncio.Semaphore(args.concurrency)
    limits = httpx.Limits(
        max_connections=args.concurrency * 2,
        max_keepalive_connections=args.concurrency,
    )

    async with httpx.AsyncClient(limits=limits) as client:
        processed = [0]
        lock = asyncio.Lock()

        async def run_one(row):
            async with sem:
                res = await process_candidate(client, row, api_key, args.archive_live)
                async with lock:
                    writer.writerow(asdict(res))
                    out_f.flush()
                    processed[0] += 1
                    if processed[0] % 25 == 0 or processed[0] == len(rows):
                        print(f"  {processed[0]}/{len(rows)}  {res.candidate_name} -> {res.verdict}")

        await asyncio.gather(*(run_one(r) for r in rows))

    out_f.close()
    print(f"Done. Output -> {args.output}")


if __name__ == "__main__":
    asyncio.run(main())
