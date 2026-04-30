"""Scrape tweets from candidate campaign X accounts via twitterapi.io.

For each candidate with required_compliance = 'Disclosure' or 'Prohibition'
and a campaign_x link, fetches tweets in the window 5 months before to
1 month after the general election date for their election year.

Tweets are stored in the `tweets` table. Images are downloaded to tweet_images/.

Usage:
    python scrape_tweets.py --api-key YOUR_KEY
    python scrape_tweets.py --api-key YOUR_KEY --year 2024
    python scrape_tweets.py --api-key YOUR_KEY --year 2024 --race "US House"
"""

from __future__ import annotations

import argparse
import calendar
import logging
import sqlite3
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
from tqdm import tqdm

from camplinks.models import DB_FILENAME

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

API_BASE = "https://api.twitterapi.io/twitter/tweet/advanced_search"
REQUEST_DELAY_S: float = 3.0
IMAGE_DIR = Path("tweet_images")


# ── Election date ─────────────────────────────────────────────────────────


def general_election_date(year: int) -> date:
    """Return the US general election date for a given year.

    The general election is the first Tuesday after the first Monday in
    November.

    Args:
        year: Election year.

    Returns:
        Date of the general election.
    """
    first_day = date(year, 11, 1)
    days_until_monday = (0 - first_day.weekday()) % 7
    first_monday = first_day + timedelta(days=days_until_monday)
    return first_monday + timedelta(days=1)


# ── Database ──────────────────────────────────────────────────────────────


def init_tweets_table(conn: sqlite3.Connection) -> None:
    """Create the tweets table if it does not exist.

    Args:
        conn: Open SQLite connection.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tweets (
            tweet_db_id         INTEGER PRIMARY KEY,
            tweet_id            TEXT    NOT NULL,
            candidate_id        INTEGER NOT NULL REFERENCES candidates(candidate_id),
            candidate_name      TEXT    NOT NULL,
            x_handle            TEXT    NOT NULL,
            created_at          TEXT,
            text                TEXT,
            like_count          INTEGER,
            retweet_count       INTEGER,
            reply_count         INTEGER,
            view_count          INTEGER,
            image_urls          TEXT,
            image_paths         TEXT,
            year                INTEGER,
            race_type           TEXT,
            required_compliance TEXT,
            UNIQUE(tweet_id)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_tweets_candidate ON tweets(candidate_id)"
    )
    conn.commit()


def insert_tweet(
    conn: sqlite3.Connection,
    tweet_id: str,
    candidate_id: int,
    candidate_name: str,
    x_handle: str,
    created_at: str,
    text: str,
    like_count: int,
    retweet_count: int,
    reply_count: int,
    view_count: int,
    image_urls: str,
    image_paths: str,
    year: int,
    race_type: str,
    required_compliance: str,
) -> None:
    """Insert a tweet row, skipping on conflict.

    Args:
        conn: Open SQLite connection.
        tweet_id: Twitter's tweet ID string.
        candidate_id: FK to candidates table.
        candidate_name: Candidate display name.
        x_handle: Twitter handle without @.
        created_at: ISO timestamp string.
        text: Tweet text.
        like_count: Number of likes.
        retweet_count: Number of retweets.
        reply_count: Number of replies.
        view_count: Number of views.
        image_urls: Comma-separated original Twitter image URLs.
        image_paths: Comma-separated local file paths of downloaded images.
        year: Election year.
        race_type: Race type from elections table.
        required_compliance: 'Disclosure' or 'Prohibition'.
    """
    conn.execute(
        """
        INSERT INTO tweets
            (tweet_id, candidate_id, candidate_name, x_handle, created_at,
             text, like_count, retweet_count, reply_count, view_count,
             image_urls, image_paths, year, race_type, required_compliance)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(tweet_id) DO NOTHING
        """,
        (
            tweet_id, candidate_id, candidate_name, x_handle, created_at,
            text, like_count, retweet_count, reply_count, view_count,
            image_urls, image_paths, year, race_type, required_compliance,
        ),
    )
    conn.commit()


def already_scraped_handles(conn: sqlite3.Connection) -> set[str]:
    """Return x_handles that already have tweets in the database.

    Args:
        conn: Open SQLite connection.

    Returns:
        Set of already-scraped handles.
    """
    rows = conn.execute("SELECT DISTINCT x_handle FROM tweets").fetchall()
    return {r[0] for r in rows}


# ── Handle extraction ─────────────────────────────────────────────────────


def extract_handle(url: str) -> str:
    """Extract Twitter/X handle from a profile URL.

    Args:
        url: X profile URL (e.g. https://x.com/candidatehandle).

    Returns:
        Handle string without @, or empty string if not parseable.
    """
    parsed = urlparse(url)
    handle = parsed.path.strip("/").split("/")[0].split("?")[0]
    return handle.lstrip("@") if handle else ""


# ── Media download ────────────────────────────────────────────────────────


def _best_video_url(media: dict) -> str:
    """Extract the highest-bitrate MP4 URL from a video media dict.

    Args:
        media: Media dict from tweet response.

    Returns:
        Best video URL string, or empty string if not found.
    """
    variants = (
        media.get("video_info", {}).get("variants", [])
        or media.get("videoInfo", {}).get("variants", [])
    )
    mp4s = [v for v in variants if v.get("content_type") == "video/mp4"]
    if not mp4s:
        return ""
    return max(mp4s, key=lambda v: v.get("bitrate", 0)).get("url", "")


def _extract_media_items(tweet: dict) -> list[dict]:
    """Find media items from all known locations in a tweet response.

    Checks extended_entities first (full media list), then entities,
    then a top-level media key, to handle twitterapi.io response variants.

    Args:
        tweet: Raw tweet dict from API response.

    Returns:
        List of media dicts, or empty list if none found.
    """
    media = (
        tweet.get("extendedEntities", {}).get("media")
        or tweet.get("extended_entities", {}).get("media")
        or tweet.get("entities", {}).get("media")
        or tweet.get("media")
        or []
    )
    if not media:
        logger.debug(
            "No media found in tweet %s. Keys: %s", tweet.get("id"), list(tweet.keys())
        )
    return media


def download_media(
    media_items: list[dict],
    candidate_id: int,
    tweet_id: str,
) -> tuple[str, str]:
    """Download images and videos from a tweet's media list.

    Args:
        media_items: List of media dicts from tweet response.
        candidate_id: Candidate ID for directory organization.
        tweet_id: Tweet ID for filename.

    Returns:
        Tuple of (comma-separated media URLs, comma-separated local file paths).
    """
    save_dir = IMAGE_DIR / str(candidate_id)
    save_dir.mkdir(parents=True, exist_ok=True)

    urls: list[str] = []
    paths: list[str] = []

    for i, media in enumerate(media_items):
        media_type = media.get("type", "photo")

        if media_type in ("video", "animated_gif"):
            url = _best_video_url(media)
            ext = ".mp4"
        else:
            url = (
                media.get("media_url_https")
                or media.get("mediaUrlHttps")
                or media.get("url", "")
            )
            ext = Path(urlparse(url).path).suffix or ".jpg"

        if not url:
            logger.debug(
                "No URL for media item %d in tweet %s: %s", i, tweet_id, media
            )
            continue

        urls.append(url)
        filename = save_dir / f"{tweet_id}_{i}{ext}"
        if filename.exists():
            paths.append(str(filename))
            continue
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            filename.write_bytes(resp.content)
            paths.append(str(filename))
            logger.info("Downloaded %s -> %s", url, filename)
        except Exception as exc:
            logger.error("Failed to download media %s: %s", url, exc)

    return ",".join(urls), ",".join(paths)


# ── API fetch ─────────────────────────────────────────────────────────────


PAGE_SIZE: int = 20


def _strip_media_urls(text: str, media_items: list[dict]) -> str:
    """Remove media t.co URLs from tweet text.

    Twitter appends a t.co short URL for each attached image at the end
    of the tweet text. This strips those so the stored text contains only
    the written content.

    Args:
        text: Raw tweet text.
        media_items: Media dicts from entities or extended_entities.

    Returns:
        Tweet text with media URLs removed and whitespace stripped.
    """
    for media in media_items:
        tco_url = media.get("url", "")
        if tco_url:
            text = text.replace(tco_url, "")
    return text.strip()


def _parse_created_at(created_at: str) -> int | None:
    """Parse a tweet createdAt string to a Unix timestamp.

    Handles both ISO 8601 and Twitter legacy format.

    Args:
        created_at: Timestamp string from the API.

    Returns:
        Unix timestamp as int, or None if unparseable.
    """
    for fmt in (
        "%a %b %d %H:%M:%S +0000 %Y",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
    ):
        try:
            dt = datetime.strptime(created_at, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            continue
    return None


def _api_request(
    params: dict[str, str | int],
    headers: dict[str, str],
    handle: str,
) -> dict:
    """Make one API request with exponential backoff on rate limits.

    Args:
        params: Query parameters for the request.
        headers: Request headers including API key.
        handle: Twitter handle (used only for logging).

    Returns:
        Parsed JSON response dict, or empty dict on failure.
    """
    retries = 5
    for attempt in range(retries):
        try:
            resp = requests.get(API_BASE, headers=headers, params=params, timeout=30)
            if resp.status_code == 429:
                wait = 30 * (2 ** attempt)
                logger.info(
                    "Rate limited for @%s, waiting %ds (attempt %d/%d)",
                    handle, wait, attempt + 1, retries,
                )
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as exc:
            logger.error("API error for @%s: %s", handle, exc)
            if attempt == retries - 1:
                return {}
            time.sleep(5)
        except Exception as exc:
            logger.error("API error for @%s: %s", handle, exc)
            return {}
    return {}


def fetch_tweets_for_handle(
    handle: str,
    since_unix: int,
    until_unix: int,
    api_key: str,
) -> list[dict]:
    """Fetch all original tweets for a handle within a time window.

    Uses time-window sliding pagination: after each page of 20 tweets,
    slides until_time back to earliest_tweet_timestamp - 1 to fetch
    the next page. Cursor-based pagination is avoided as it causes
    infinite loops on historical data per twitterapi.io documentation.

    Args:
        handle: Twitter handle without @.
        since_unix: Start of window as Unix timestamp (seconds).
        until_unix: End of window as Unix timestamp (seconds).
        api_key: twitterapi.io API key.

    Returns:
        List of raw tweet dicts.
    """
    headers = {"x-api-key": api_key}
    all_tweets: list[dict] = []
    current_until = until_unix

    while current_until > since_unix:
        query = (
            f"from:{handle} since_time:{since_unix} until_time:{current_until} "
            f"-filter:replies -filter:retweets"
        )
        params: dict[str, str | int] = {"query": query, "queryType": "Latest"}

        time.sleep(REQUEST_DELAY_S)
        data = _api_request(params, headers, handle)
        tweets = data.get("tweets", [])

        if not tweets:
            break

        all_tweets.extend(tweets)

        # Find earliest tweet timestamp to slide the window back
        timestamps = [
            ts for t in tweets
            if (ts := _parse_created_at(t.get("createdAt", ""))) is not None
        ]
        if not timestamps or len(tweets) < PAGE_SIZE:
            break

        current_until = min(timestamps) - 1

    return all_tweets


# ── Orchestrator ──────────────────────────────────────────────────────────


def scrape_tweets(
    api_key: str,
    db_path: str = DB_FILENAME,
    year: int | None = None,
    race_type: str | None = None,
    candidate_name: str | None = None,
) -> int:
    """Scrape tweets for all compliance-flagged candidates with X links.

    Args:
        api_key: twitterapi.io API key.
        db_path: Path to the SQLite database.
        year: Optional filter by election year.
        race_type: Optional filter by race type.
        candidate_name: Optional filter by candidate name (case-insensitive substring match).

    Returns:
        Total number of tweets saved.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    init_tweets_table(conn)

    query = """
        SELECT
            c.candidate_id,
            c.candidate_name,
            c.required_compliance,
            cl.url AS x_url,
            e.year,
            e.race_type
        FROM candidates c
        JOIN elections e ON c.election_id = e.election_id
        JOIN contact_links cl ON cl.candidate_id = c.candidate_id
        WHERE cl.link_type = 'campaign_x'
          AND c.required_compliance IN ('Disclosure', 'Prohibition')
          AND c.candidate_name != ''
    """
    params: list[str | int] = []
    if year is not None:
        query += " AND e.year = ?"
        params.append(year)
    if race_type is not None:
        query += " AND e.race_type = ?"
        params.append(race_type)
    if candidate_name is not None:
        query += " AND c.candidate_name LIKE ?"
        params.append(f"%{candidate_name}%")

    rows = conn.execute(query, params).fetchall()
    done_handles = already_scraped_handles(conn)
    remaining = [r for r in rows if extract_handle(r["x_url"]) not in done_handles]

    logger.info(
        "Found %d candidates. %d already scraped. %d remaining.",
        len(rows),
        len(rows) - len(remaining),
        len(remaining),
    )

    total_saved = 0

    for row in tqdm(remaining, desc="Scraping tweets", unit="candidate"):
        handle = extract_handle(row["x_url"])
        if not handle:
            logger.error("Could not parse handle from %s", row["x_url"])
            continue

        election_year = row["year"]
        election_date = general_election_date(election_year)
        since_date = election_date - timedelta(days=150)
        until_date = election_date + timedelta(days=30)

        since_unix = int(calendar.timegm(since_date.timetuple()))
        until_unix = int(calendar.timegm(until_date.timetuple()))

        logger.info(
            "Scraping @%s (%s, %s %d) | %s to %s",
            handle,
            row["candidate_name"],
            row["race_type"],
            election_year,
            since_date,
            until_date,
        )

        tweets = fetch_tweets_for_handle(handle, since_unix, until_unix, api_key)

        for tweet in tweets:
            tweet_id = str(tweet.get("id", ""))
            if not tweet_id:
                continue

            media_items = _extract_media_items(tweet)
            image_urls, image_paths = download_media(
                media_items, row["candidate_id"], tweet_id
            )

            insert_tweet(
                conn,
                tweet_id=tweet_id,
                candidate_id=row["candidate_id"],
                candidate_name=row["candidate_name"],
                x_handle=handle,
                created_at=tweet.get("createdAt", ""),
                text=_strip_media_urls(tweet.get("text", ""), media_items),
                like_count=tweet.get("likeCount", 0),
                retweet_count=tweet.get("retweetCount", 0),
                reply_count=tweet.get("replyCount", 0),
                view_count=tweet.get("viewCount", 0),
                image_urls=image_urls,
                image_paths=image_paths,
                year=election_year,
                race_type=row["race_type"],
                required_compliance=row["required_compliance"],
            )
            total_saved += 1

        logger.info("Saved %d tweets for @%s.", len(tweets), handle)

    conn.close()
    logger.info("Done. Total tweets saved: %d", total_saved)
    return total_saved


# ── CLI ───────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape candidate tweets.")
    parser.add_argument("--api-key", required=True, help="twitterapi.io API key")
    parser.add_argument("--db", default=DB_FILENAME, help="Database path")
    parser.add_argument("--year", type=int, default=None, help="Filter by election year")
    parser.add_argument("--race", default=None, help="Filter by race type")
    parser.add_argument("--candidate", default=None, help="Filter by candidate name (case-insensitive substring)")
    args = parser.parse_args()

    scrape_tweets(
        api_key=args.api_key,
        db_path=args.db,
        year=args.year,
        race_type=args.race,
        candidate_name=args.candidate,
    )
