## attempts at scraping campaign website information
import re
import random
import sqlite3
import time

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

CAMPLINKS_DB = "/Users/agueorg/Desktop/WeberLab/campLinks/camplinks.db"

#load candidate ids and URLs
def load_candidates_from_db(db_path: str) -> list[dict]:
    """Load candidate IDs, names, and campaign site URLs from camplinks.db.

    Returns:
        List of dicts with keys: candidate_id, candidate_name, url.
    """
    con = sqlite3.connect(db_path)
    try:  # selects only URLs from database that are of type campaign_site
        rows = con.execute(
            """
            SELECT c.candidate_id, c.candidate_name, cl.url
            FROM candidates c
            JOIN contact_links cl ON c.candidate_id = cl.candidate_id
            WHERE cl.link_type = 'campaign_site'
              AND cl.url IS NOT NULL
              AND cl.url != ''
            """
        ).fetchall()
    finally:
        con.close()
    return [{"candidate_id": cid, "candidate_name": name, "url": url} for cid, name, url in rows]


def init_content_table(con: sqlite3.Connection) -> None:
    """Create the content table in camplinks.db if it does not exist.

    Args:
        con: Open SQLite connection.
    """
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS content (
            content_id       INTEGER PRIMARY KEY,
            candidate_id     INTEGER NOT NULL REFERENCES candidates(candidate_id),
            candidate_name   TEXT    NOT NULL,
            page_url         TEXT    NOT NULL,
            page_type        TEXT    NOT NULL,
            unprocessed_text TEXT,
            cleaned_text     TEXT,
            sampled_text     TEXT,
            UNIQUE(candidate_id, page_url)
        )
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_content_candidate ON content(candidate_id)"
    )
    con.commit()


def insert_content(
    con: sqlite3.Connection,
    candidate_id: int,
    candidate_name: str,
    page_url: str,
    page_type: str,
    unprocessed_text: str,
    cleaned_text: str,
    sampled_text: str,
) -> None:
    """Insert or replace a scraped page into the content table.

    Args:
        con: Open SQLite connection.
        candidate_id: FK to candidates table.
        candidate_name: Display name of the candidate.
        page_url: URL of the scraped page.
        page_type: One of 'home', 'policy', or 'about'.
        unprocessed_text: Raw visible text from the page.
        cleaned_text: Text after character cleaning.
        sampled_text: Random sentence-chunk sample of cleaned_text.
    """
    con.execute(
        """
        INSERT INTO content
            (candidate_id, candidate_name, page_url, page_type,
             unprocessed_text, cleaned_text, sampled_text)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_id, page_url) DO NOTHING
        """,
        (candidate_id, candidate_name, page_url, page_type,
         unprocessed_text, cleaned_text, sampled_text),
    )
    con.commit()

def load_scraped_candidate_ids(con: sqlite3.Connection) -> set[int]:
    """Return the set of candidate_ids that already have rows in the content table.

    Args:
        con: Open SQLite connection.

    Returns:
        Set of candidate_id integers already scraped.
    """
    rows = con.execute("SELECT DISTINCT candidate_id FROM content").fetchall()
    return {row[0] for row in rows}


data = load_candidates_from_db(CAMPLINKS_DB)

# Tags whose content is never visible to a reader
INVISIBLE_TAGS = {"script", "style", "noscript", "head", "meta", "link", "template"}

# URL path keywords that suggest a policy/agenda subpage
POLICY_KEYWORDS = {
    "issue", "issues", "policy", "policies", "platform", "agenda",
    "priorities", "priority", "positions", "position", "plans", "plan",
    "vision", "values", "focus", "reform"
}

ABOUT_KEYWORDS = {
    "about", "meet"
}

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; research-scraper/1.0)"}

#DB_PATH   = "/Users/agueorg/Desktop/WeberLab/anna-RA/candidate-scraping/media_db.db"

#We decided to only scrape text data, thus code related to scraping image and video is commented out 
#MEDIA_DIR = "/Users/agueorg/Desktop/WeberLab/anna-RA/candidate-scraping/candidate_media"
#VIDEO_EXTENSIONS = {".mp4", ".webm", ".mov", ".avi", ".mkv", ".m4v"}
#EMBEDDED_VIDEO_DOMAINS = {"youtube.com", "youtu.be", "vimeo.com"}

def fetch_soup(url):
    """Fetch a URL and return a BeautifulSoup object, or None on failure."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")
    except Exception:
        return None

def extract_visible_text(soup):
    """Strip invisible tags and return cleaned visible text."""
    for tag in soup(INVISIBLE_TAGS):
        tag.decompose()
    raw_text = soup.get_text(separator=" ") #get_text gets the actual text that gets read
    lines = [line.strip() for line in raw_text.splitlines()]
    return " ".join(line for line in lines if line)

def find_policy_links(soup, base_url):
    """
    Find internal links whose path contains a policy-related keyword.
    Returns a deduplicated list of absolute URLs.
    """
    base_domain = urlparse(base_url).netloc
    seen = set()
    links = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        # Must be same domain, http/https, and not already visited
        if parsed.scheme not in ("http", "https"):
            continue
        if parsed.netloc != base_domain:
            continue
        if absolute in seen:
            continue
        # Check each path segment against policy keywords
        path_parts = set(parsed.path.lower().strip("/").split("/"))
        if path_parts & POLICY_KEYWORDS:
            seen.add(absolute)
            links.append(absolute)
    return links

def find_about_links(soup, base_url):
    """
    Find internal links whose path contains an about-page related keyword.
    Returns a deduplicated list of absolute URLs.
    """
    base_domain = urlparse(base_url).netloc
    seen = set()
    links = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        # Must be same domain, http/https, and not already visited
        if parsed.scheme not in ("http", "https"):
            continue
        if parsed.netloc != base_domain:
            continue
        if absolute in seen:
            continue
        # Check each path segment against policy keywords
        path_parts = set(parsed.path.lower().strip("/").split("/"))
        if path_parts & ABOUT_KEYWORDS:
            seen.add(absolute)
            links.append(absolute)
    return links

#not scraping image and video
''' 
def collect_media_urls(soup, page_url):
    """Return sets of image URLs, downloadable video URLs, and embedded video URLs."""
    images, videos, embedded = set(), set(), set()

    for img in soup.find_all("img", src=True):
        src = urljoin(page_url, img["src"].strip())
        if urlparse(src).scheme in ("http", "https"):
            images.add(src)

    for video in soup.find_all("video"):
        if video.get("src"):
            videos.add(urljoin(page_url, video["src"].strip()))
        for source in video.find_all("source", src=True):
            videos.add(urljoin(page_url, source["src"].strip()))

    for a in soup.find_all("a", href=True):
        href = urljoin(page_url, a["href"].strip())
        if os.path.splitext(urlparse(href).path)[1].lower() in VIDEO_EXTENSIONS:
            videos.add(href)

    for iframe in soup.find_all("iframe", src=True):
        domain = urlparse(iframe["src"]).netloc.replace("www.", "")
        if any(d in domain for d in EMBEDDED_VIDEO_DOMAINS):
            embedded.add(iframe["src"].strip())

    return images, videos, embedded


def download_candidate_media(campaign_url, candidate_name, home_soup, db_conn, candidate_id):
    """Download all images and videos from a candidate's site into a named folder
    and record each asset in the database."""
    folder = os.path.join(MEDIA_DIR, re.sub(r"[^\w\s-]", "", candidate_name).strip().replace(" ", "_"))
    os.makedirs(folder, exist_ok=True)

    pages = [(campaign_url, home_soup)] + [
        (link, fetch_soup(link)) for link in find_policy_links(home_soup, campaign_url)
    ] +  [
        (link, fetch_soup(link)) for link in find_about_links(home_soup, campaign_url)
    ]

    # Maps source_url -> page_url; setdefault keeps the first page that found it.
    all_images, all_videos, all_embedded = {}, {}, {}
    for page_url, soup in pages:
        if soup is None:
            continue
        imgs, vids, emb = collect_media_urls(soup, page_url)
        for u in imgs:
            all_images.setdefault(u, page_url)
        for u in vids:
            all_videos.setdefault(u, page_url)
        for u in emb:
            all_embedded.setdefault(u, page_url)
        time.sleep(1)

    for url, page_url in {**all_images, **all_videos}.items():
        media_type = "image" if url in all_images else "video"
        local_path = None
        try:
            filename = os.path.basename(urlparse(url).path) or "file"
            filepath = os.path.join(folder, filename)
            if not os.path.exists(filepath):
                r = requests.get(url, headers=HEADERS, timeout=15, stream=True)
                r.raise_for_status()
                with open(filepath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            local_path = filepath
        except Exception as e:
            print(f"  Failed to download {url}: {e}")
        media_db.insert_media(db_conn, candidate_id, candidate_name, media_type, url,
                              local_path=local_path, page_url=page_url)

    for url, page_url in all_embedded.items():
        media_db.insert_media(db_conn, candidate_id, "embedded", url, page_url=page_url)

    print(f"  Media: {len(all_images)} images, {len(all_videos)} videos, {len(all_embedded)} embedded -> {folder}")

'''

def scrape_candidate(campaign_url, home_soup=None):
    """
    Scrape the home page and any policy/agenda subpages.
    Accepts an already-fetched home_soup to avoid re-fetching.
    Returns a list of dicts with keys: page_url, page_type, visible_text.
    """
    if not isinstance(campaign_url, str) or campaign_url.strip() == "":
        return []

    pages = []

    # --- Home page ---
    if home_soup is None:
        home_soup = fetch_soup(campaign_url)
    if home_soup is None:
        return [{"page_url": campaign_url, "page_type": "home", "visible_text": "ERROR: could not fetch page"}]

    pages.append({
        "page_url": campaign_url,
        "page_type": "home",
        "visible_text": extract_visible_text(home_soup),
    })

    # --- Policy / agenda subpages ---
    policy_links = find_policy_links(home_soup, campaign_url)
    for link in policy_links:
        print(f"  -> subpage policy: {link}")
        sub_soup = fetch_soup(link)
        if sub_soup is None:
            continue
        pages.append({
                "page_url": link,
                "page_type": "policy",
                "visible_text": extract_visible_text(sub_soup),
            })
        time.sleep(1)
    
    about_links = find_about_links(home_soup, campaign_url)
    for link in about_links:
        print(f"  -> subpage about: {link}")
        sub_soup = fetch_soup(link)
        if sub_soup is None:
            continue
        pages.append({
                "page_url": link,
                "page_type": "about",
                "visible_text": extract_visible_text(sub_soup),
            })
        time.sleep(1)

    return pages

def clean_text(text):
    """Remove characters that are not letters, numbers, whitespace, or common punctuation."""
    if not isinstance(text, str):
        return text
    return re.sub(r"[^a-zA-Z0-9\s!@#$%&*()\:.,?'\"-]", "", text)

def sample_text(text, fraction=0.4, max_attempts=5):
    """Return a contiguous chunk of ~40% of sentences, starting at a random sentence.
    Retries if the sampled chunk contains 'ERROR'."""
    if not isinstance(text, str) or text.startswith("ERROR:") or text == "":
        return text
    sentences = [s.strip() for s in re.split(r'(?<=[.!?])\s+', text) if s.strip()]
    if not sentences:
        return text
    k = max(1, int(len(sentences) * fraction))
    max_start = max(0, len(sentences) - k) #caps the starting point so that there is never less than 40% collected based on starting point
    for _ in range(max_attempts): #checks to see if there is ERROR contained in the string. Likely though, the entire text is just ERROR if it exists 
        start = random.randint(0, max_start)
        sample = " ".join(sentences[start:start + k])
        if "ERROR" not in sample:
            return sample
    return sample  # return last attempt if all contain ERROR

# putting it all together:
db_conn = sqlite3.connect(CAMPLINKS_DB)
db_conn.execute("PRAGMA foreign_keys = ON")
init_content_table(db_conn)

already_scraped = load_scraped_candidate_ids(db_conn)
remaining = [r for r in data if r["candidate_id"] not in already_scraped]
print(f"Skipping {len(data) - len(remaining)} already-scraped candidates. {len(remaining)} remaining.")

for row in remaining[:150]:
    url = row["url"]
    candidate_id = row["candidate_id"]
    candidate_name_temp = row["candidate_name"]
    print(f"Scraping: {url}")
    home_soup = fetch_soup(url) if isinstance(url, str) and url.strip() else None

    for page in scrape_candidate(url, home_soup=home_soup):
        ct = clean_text(page["visible_text"])
        insert_content(
            db_conn,
            candidate_id=candidate_id,
            candidate_name=candidate_name_temp,
            page_url=page["page_url"],
            page_type=page["page_type"],
            unprocessed_text=page["visible_text"],
            cleaned_text=ct,
            sampled_text=sample_text(ct),
        )

    time.sleep(1)  # be polite between candidates

db_conn.close()
print(f"Done. Results saved to {CAMPLINKS_DB}")
