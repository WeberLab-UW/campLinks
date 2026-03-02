"""Tests for the Ballotpedia gubernatorial scraper."""

from __future__ import annotations

from bs4 import BeautifulSoup

from camplinks.scrapers.ballotpedia_governor import (
    BallotpediaGovernorScraper,
    _FALLBACK_STATES,
)
from camplinks.scrapers.ballotpedia_parsing import BALLOTPEDIA_BASE


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


# ── HTML fixtures ─────────────────────────────────────────────────────────


GOVERNOR_VOTEBOX_HTML = """\
<div class="votebox">
  <div class="race_header">
    <h5 class="votebox-header-election-type">General election for Governor of Delaware</h5>
  </div>
  <table>
    <tr class="results_row winner">
      <td class="votebox-results-cell--text">
        <a href="/Matt_Meyer">Matt Meyer</a> (Democratic)
      </td>
      <td class="votebox-results-cell--number">
        <span class="percentage_number">56.3%</span>
      </td>
      <td class="votebox-results-cell--number">230,000</td>
    </tr>
    <tr class="results_row">
      <td class="votebox-results-cell--text">
        <a href="/Mike_Ramone">Mike Ramone</a> (Republican)
      </td>
      <td class="votebox-results-cell--number">
        <span class="percentage_number">43.7%</span>
      </td>
      <td class="votebox-results-cell--number">178,000</td>
    </tr>
  </table>
</div>
"""

PRIMARY_VOTEBOX_HTML = """\
<div class="votebox">
  <div class="race_header">
    <h5 class="votebox-header-election-type">Republican primary election for Governor of Texas</h5>
  </div>
  <table>
    <tr class="results_row winner">
      <td class="votebox-results-cell--text">
        <a href="/Greg_Abbott">Greg Abbott</a> (Republican)
      </td>
      <td class="votebox-results-cell--number">
        <span class="percentage_number">66.2%</span>
      </td>
      <td class="votebox-results-cell--number">1,500,000</td>
    </tr>
    <tr class="results_row">
      <td class="votebox-results-cell--text">
        <a href="/Don_Huffines">Don Huffines</a> (Republican)
      </td>
      <td class="votebox-results-cell--number">
        <span class="percentage_number">12.3%</span>
      </td>
      <td class="votebox-results-cell--number">280,000</td>
    </tr>
  </table>
</div>
"""

MULTI_STAGE_HTML = f"""\
<html><body>
{PRIMARY_VOTEBOX_HTML}
{GOVERNOR_VOTEBOX_HTML.replace("Delaware", "Texas").replace("Matt Meyer", "Greg Abbott").replace("/Matt_Meyer", "/Greg_Abbott").replace("Mike Ramone", "Beto O'Rourke").replace("/Mike_Ramone", "/Beto_ORourke")}
</body></html>
"""

INDEX_TABLE_HTML = """\
<html><body>
<table class="sortable">
  <tr>
    <th>State</th>
    <th>Incumbent</th>
    <th>Term-limited</th>
  </tr>
  <tr>
    <td><a href="/Delaware_gubernatorial_election,_2024">Delaware</a></td>
    <td>John Carney</td>
    <td>Y</td>
  </tr>
  <tr>
    <td><a href="/Indiana_gubernatorial_and_lieutenant_gubernatorial_election,_2024">Indiana</a></td>
    <td>Eric Holcomb</td>
    <td>Y</td>
  </tr>
  <tr>
    <td><a href="/Missouri_gubernatorial_election,_2024">Missouri</a></td>
    <td>Mike Parson</td>
    <td>Y</td>
  </tr>
  <tr>
    <td><a href="/New_Hampshire_gubernatorial_election,_2024">New Hampshire</a></td>
    <td>Chris Sununu</td>
    <td>N</td>
  </tr>
</table>
</body></html>
"""


# ── TestCollectStateUrls ──────────────────────────────────────────────────


class TestCollectStateUrls:
    """Tests for collect_state_urls()."""

    def test_extracts_states(self) -> None:
        scraper = BallotpediaGovernorScraper()
        soup = _soup(INDEX_TABLE_HTML)
        results = scraper.collect_state_urls(soup, 2024)
        # Should find 3 states (Indiana excluded due to lt gov in URL)
        state_names = [r[0] for r in results]
        assert "Delaware" in state_names
        assert "Missouri" in state_names
        assert "New Hampshire" in state_names

    def test_excludes_lieutenant_governor(self) -> None:
        scraper = BallotpediaGovernorScraper()
        soup = _soup(INDEX_TABLE_HTML)
        results = scraper.collect_state_urls(soup, 2024)
        state_names = [r[0] for r in results]
        assert "Indiana" not in state_names

    def test_url_construction(self) -> None:
        scraper = BallotpediaGovernorScraper()
        soup = _soup(INDEX_TABLE_HTML)
        results = scraper.collect_state_urls(soup, 2024)
        urls = {r[0]: r[1] for r in results}
        assert urls["Delaware"] == (
            f"{BALLOTPEDIA_BASE}/Delaware_gubernatorial_election,_2024"
        )
        assert urls["New Hampshire"] == (
            f"{BALLOTPEDIA_BASE}/New_Hampshire_gubernatorial_election,_2024"
        )

    def test_fallback_on_empty_page(self) -> None:
        scraper = BallotpediaGovernorScraper()
        soup = _soup("<html><body></body></html>")
        results = scraper.collect_state_urls(soup, 2024)
        assert len(results) == 50

    def test_no_duplicates(self) -> None:
        # Duplicate links in HTML
        html = INDEX_TABLE_HTML.replace(
            "</table>",
            '<tr><td><a href="/Delaware_gubernatorial_election,_2024">Delaware</a></td></tr></table>',
        )
        scraper = BallotpediaGovernorScraper()
        soup = _soup(html)
        results = scraper.collect_state_urls(soup, 2024)
        states = [r[0] for r in results]
        assert states.count("Delaware") == 1


# ── TestParseStatePage ────────────────────────────────────────────────────


class TestParseStatePage:
    """Tests for parse_state_page()."""

    def test_parses_general_election(self) -> None:
        scraper = BallotpediaGovernorScraper()
        soup = _soup(f"<html><body>{GOVERNOR_VOTEBOX_HTML}</body></html>")
        results = scraper.parse_state_page("Delaware", soup, 2024)
        assert len(results) == 1
        election, candidates = results[0]
        assert election.state == "Delaware"
        assert election.race_type == "Governor"
        assert election.year == 2024
        assert election.election_stage == "general"
        assert len(candidates) == 2

    def test_parses_primary_election(self) -> None:
        scraper = BallotpediaGovernorScraper()
        soup = _soup(f"<html><body>{PRIMARY_VOTEBOX_HTML}</body></html>")
        results = scraper.parse_state_page("Texas", soup, 2026)
        assert len(results) == 1
        election, candidates = results[0]
        assert election.election_stage == "primary"
        assert len(candidates) == 2

    def test_multi_stage_page(self) -> None:
        scraper = BallotpediaGovernorScraper()
        soup = _soup(MULTI_STAGE_HTML)
        results = scraper.parse_state_page("Texas", soup, 2026)
        assert len(results) == 2
        stages = {r[0].election_stage for r in results}
        assert stages == {"general", "primary"}

    def test_empty_page(self) -> None:
        scraper = BallotpediaGovernorScraper()
        soup = _soup("<html><body><p>No election data.</p></body></html>")
        results = scraper.parse_state_page("Alaska", soup, 2026)
        assert results == []

    def test_winner_detection(self) -> None:
        scraper = BallotpediaGovernorScraper()
        soup = _soup(f"<html><body>{GOVERNOR_VOTEBOX_HTML}</body></html>")
        results = scraper.parse_state_page("Delaware", soup, 2024)
        _, candidates = results[0]
        winners = [c for c in candidates if c.is_winner]
        assert len(winners) == 1
        assert winners[0].candidate_name == "Matt Meyer"

    def test_district_is_none(self) -> None:
        scraper = BallotpediaGovernorScraper()
        soup = _soup(f"<html><body>{GOVERNOR_VOTEBOX_HTML}</body></html>")
        results = scraper.parse_state_page("Delaware", soup, 2024)
        assert results[0][0].district is None


# ── TestBuildIndexUrl ─────────────────────────────────────────────────────


class TestBuildIndexUrl:
    """Tests for build_index_url()."""

    def test_url_format(self) -> None:
        scraper = BallotpediaGovernorScraper()
        assert scraper.build_index_url(2026) == (
            f"{BALLOTPEDIA_BASE}/Gubernatorial_elections,_2026"
        )

    def test_url_uses_comma_format(self) -> None:
        scraper = BallotpediaGovernorScraper()
        url = scraper.build_index_url(2024)
        # Must use comma before year (Ballotpedia convention)
        assert ",_2024" in url


# ── TestBuildElectionUrl ──────────────────────────────────────────────────


class TestBuildElectionUrl:
    """Tests for _build_election_url()."""

    def test_simple_state(self) -> None:
        url = BallotpediaGovernorScraper._build_election_url("Texas", 2026)
        assert url == f"{BALLOTPEDIA_BASE}/Texas_gubernatorial_election,_2026"

    def test_multi_word_state(self) -> None:
        url = BallotpediaGovernorScraper._build_election_url("New Hampshire", 2024)
        assert url == (f"{BALLOTPEDIA_BASE}/New_Hampshire_gubernatorial_election,_2024")


# ── TestFallbackStates ────────────────────────────────────────────────────


class TestFallbackStates:
    """Tests for the fallback states list."""

    def test_contains_50_states(self) -> None:
        assert len(_FALLBACK_STATES) == 50

    def test_includes_key_states(self) -> None:
        assert "Texas" in _FALLBACK_STATES
        assert "California" in _FALLBACK_STATES
        assert "New York" in _FALLBACK_STATES

    def test_no_territories(self) -> None:
        assert "Guam" not in _FALLBACK_STATES
        assert "Puerto Rico" not in _FALLBACK_STATES
