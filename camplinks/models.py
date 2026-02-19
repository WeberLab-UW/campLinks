"""Shared data containers for the camplinks pipeline."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Election:
    """A single political race (one contest in one district/state).

    Attributes:
        state: US state or territory name.
        race_type: E.g. "US House", "US Senate", "Governor".
        year: Election year.
        district: District identifier or None for statewide races.
        wikipedia_url: Source Wikipedia page URL.
        election_id: Database primary key (set after insertion).
    """

    state: str
    race_type: str
    year: int
    district: str | None = None
    wikipedia_url: str = ""
    election_id: int | None = None


@dataclass
class Candidate:
    """A candidate running in an election.

    Attributes:
        party: Party name (e.g. "Republican", "Democratic", "Libertarian").
        candidate_name: Full name.
        wikipedia_url: Wikipedia page URL, if available.
        ballotpedia_url: Ballotpedia page URL, if discovered.
        vote_pct: Vote share percentage, if available.
        is_winner: Whether this candidate won the race.
        candidate_id: Database primary key (set after insertion).
        election_id: Foreign key to the parent election (set after insertion).
    """

    party: str
    candidate_name: str
    wikipedia_url: str = ""
    ballotpedia_url: str = ""
    vote_pct: float | None = None
    is_winner: bool = False
    candidate_id: int | None = None
    election_id: int | None = None


@dataclass
class ContactLink:
    """A single contact/social link for a candidate.

    Attributes:
        candidate_id: Foreign key to the candidate.
        link_type: E.g. "campaign_site", "campaign_facebook".
        url: The link URL.
        source: How this link was discovered ("wikipedia", "ballotpedia",
            "web_search").
    """

    candidate_id: int
    link_type: str
    url: str
    source: str


# Valid link_type values
LINK_TYPES: tuple[str, ...] = (
    "campaign_site",
    "campaign_facebook",
    "campaign_x",
    "campaign_instagram",
    "personal_website",
    "personal_facebook",
    "personal_linkedin",
)

# Mapping from Ballotpedia contact labels to our link_type values
BALLOTPEDIA_LABEL_MAP: dict[str, str] = {
    "campaign website": "campaign_site",
    "campaign facebook": "campaign_facebook",
    "campaign x": "campaign_x",
    "campaign instagram": "campaign_instagram",
    "personal website": "personal_website",
    "personal facebook": "personal_facebook",
    "personal linkedin": "personal_linkedin",
}

# Default database filename
DB_FILENAME = "camplinks.db"

# Default CSV filenames (legacy)
RACES_CSV = "races.csv"
CANDIDATES_CSV = "candidates.csv"
CONTACT_LINKS_CSV = "contact_links.csv"
