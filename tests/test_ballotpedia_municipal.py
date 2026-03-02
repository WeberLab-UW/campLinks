"""Tests for the Ballotpedia municipal scraper."""

from __future__ import annotations

from bs4 import BeautifulSoup

from camplinks.scrapers.ballotpedia_municipal import (
    TOP_100_URL,
    BallotpediaMunicipalScraper,
)
from camplinks.scrapers.ballotpedia_parsing import (
    BALLOTPEDIA_BASE,
    detect_election_stage,
    parse_candidate_cell,
    parse_rcv_votebox,
    parse_votebox,
)


# ── HTML fixtures ─────────────────────────────────────────────────────────


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


VOTEBOX_HTML = """\
<div class="votebox">
  <div class="race_header">
    <h5 class="votebox-header-election-type">General election for Mayor of Houston</h5>
  </div>
  <table>
    <tr class="results_row winner">
      <td class="votebox-results-cell--text">
        <a href="/John_Whitmire">John Whitmire</a> (Nonpartisan)
      </td>
      <td class="votebox-results-cell--number">
        <span class="percentage_number">64.4%</span>
      </td>
      <td class="votebox-results-cell--number">129,809</td>
    </tr>
    <tr class="results_row">
      <td class="votebox-results-cell--text">
        <a href="/Sheila_Jackson_Lee">Sheila Jackson Lee</a> (Nonpartisan)
      </td>
      <td class="votebox-results-cell--number">
        <span class="percentage_number">35.6%</span>
      </td>
      <td class="votebox-results-cell--number">71,912</td>
    </tr>
  </table>
</div>
"""

RUNOFF_VOTEBOX_HTML = """\
<div class="votebox">
  <div class="race_header">
    <h5 class="votebox-header-election-type">General runoff election for Mayor of Jacksonville</h5>
  </div>
  <table>
    <tr class="results_row winner">
      <td class="votebox-results-cell--text">
        <a href="/Donna_Deegan">Donna Deegan</a> (D)
      </td>
      <td class="votebox-results-cell--number">
        <span class="percentage_number">52.1%</span>
      </td>
      <td class="votebox-results-cell--number">113,226</td>
    </tr>
    <tr class="results_row">
      <td class="votebox-results-cell--text">
        <a href="/Daniel_Davis">Daniel Davis</a> (R)
      </td>
      <td class="votebox-results-cell--number">
        <span class="percentage_number">47.9%</span>
      </td>
      <td class="votebox-results-cell--number">104,172</td>
    </tr>
  </table>
</div>
"""

RCV_VOTEBOX_HTML = """\
<div class="rcvvotebox">
  <div class="race_header">
    <h5 class="votebox-header-election-type">General election for Mayor of San Francisco</h5>
  </div>
  <table class="round-1">
    <tr class="results_row">
      <td class="votebox-results-cell--text">
        <a href="/Daniel_Lurie">Daniel Lurie</a> (Nonpartisan)
      </td>
      <td class="votebox-results-cell--number">
        <span class="percentage_number">30.2%</span>
      </td>
      <td class="votebox-results-cell--number">50,000</td>
    </tr>
    <tr class="results_row">
      <td class="votebox-results-cell--text">
        <a href="/London_Breed">London Breed</a> (Nonpartisan)
      </td>
      <td class="votebox-results-cell--number">
        <span class="percentage_number">28.5%</span>
      </td>
      <td class="votebox-results-cell--number">47,000</td>
    </tr>
  </table>
  <table class="round-final">
    <tr class="results_row winner">
      <td class="votebox-results-cell--text">
        <a href="/Daniel_Lurie">Daniel Lurie</a> (Nonpartisan)
      </td>
      <td class="votebox-results-cell--number">
        <span class="percentage_number">55.0%</span>
      </td>
      <td class="votebox-results-cell--number">182,364</td>
    </tr>
    <tr class="results_row">
      <td class="votebox-results-cell--text">
        <a href="/London_Breed">London Breed</a> (Nonpartisan)
      </td>
      <td class="votebox-results-cell--number">
        <span class="percentage_number">45.0%</span>
      </td>
      <td class="votebox-results-cell--number">149,200</td>
    </tr>
  </table>
</div>
"""

MULTI_STAGE_HTML = f"""\
<html><body>
{RUNOFF_VOTEBOX_HTML}
{VOTEBOX_HTML.replace("Houston", "Jacksonville").replace("John Whitmire", "Donna Deegan").replace("/John_Whitmire", "/Donna_Deegan").replace("Sheila Jackson Lee", "Daniel Davis").replace("/Sheila_Jackson_Lee", "/Daniel_Davis")}
</body></html>
"""

TOP_100_TABLE_HTML = """\
<html><body>
<table class="wikitable sortable">
  <tr>
    <th>Rank</th>
    <th>City</th>
    <th>Population</th>
  </tr>
  <tr>
    <td>1</td>
    <td><a href="/New_York,_New_York">New York, New York</a></td>
    <td>8,336,817</td>
  </tr>
  <tr>
    <td>2</td>
    <td><a href="/Los_Angeles,_California">Los Angeles, California</a></td>
    <td>3,979,576</td>
  </tr>
  <tr>
    <td>3</td>
    <td><a href="/St._Louis,_Missouri">St. Louis, Missouri</a></td>
    <td>301,578</td>
  </tr>
</table>
</body></html>
"""


# ── TestParseCandidateCell ────────────────────────────────────────────────


class TestParseCandidateCell:
    """Tests for parse_candidate_cell()."""

    def test_standard_party(self) -> None:
        name, party = parse_candidate_cell("John Smith (Democrat)")
        assert name == "John Smith"
        assert party == "Democrat"

    def test_nonpartisan(self) -> None:
        name, party = parse_candidate_cell("Jane Doe (Nonpartisan)")
        assert name == "Jane Doe"
        assert party == "Nonpartisan"

    def test_abbreviated_party(self) -> None:
        name, party = parse_candidate_cell("Bob Jones (R)")
        assert name == "Bob Jones"
        assert party == "R"

    def test_no_party(self) -> None:
        name, party = parse_candidate_cell("Alice Wonder")
        assert name == "Alice Wonder"
        assert party == ""

    def test_whitespace_handling(self) -> None:
        name, party = parse_candidate_cell("  John Smith (D)  ")
        assert name == "John Smith"
        assert party == "D"


# ── TestDetectElectionStage ───────────────────────────────────────────────


class TestDetectElectionStage:
    """Tests for detect_election_stage()."""

    def test_general_election(self) -> None:
        soup = _soup(VOTEBOX_HTML)
        vbox = soup.find("div", class_="votebox")
        assert detect_election_stage(vbox) == "general"

    def test_runoff_election(self) -> None:
        soup = _soup(RUNOFF_VOTEBOX_HTML)
        vbox = soup.find("div", class_="votebox")
        assert detect_election_stage(vbox) == "runoff"

    def test_primary_election(self) -> None:
        html = VOTEBOX_HTML.replace("General election", "Democratic primary election")
        soup = _soup(html)
        vbox = soup.find("div", class_="votebox")
        assert detect_election_stage(vbox) == "primary"

    def test_no_header_defaults_to_general(self) -> None:
        html = '<div class="votebox"><table></table></div>'
        soup = _soup(html)
        vbox = soup.find("div", class_="votebox")
        assert detect_election_stage(vbox) == "general"


# ── TestParseVotebox ──────────────────────────────────────────────────────


class TestParseVotebox:
    """Tests for parse_votebox()."""

    def test_extracts_candidates(self) -> None:
        soup = _soup(VOTEBOX_HTML)
        vbox = soup.find("div", class_="votebox")
        parsed = parse_votebox(vbox)
        assert len(parsed) == 2

    def test_winner_detection(self) -> None:
        soup = _soup(VOTEBOX_HTML)
        vbox = soup.find("div", class_="votebox")
        parsed = parse_votebox(vbox)
        assert parsed[0]["is_winner"] is True
        assert parsed[1]["is_winner"] is False

    def test_candidate_name(self) -> None:
        soup = _soup(VOTEBOX_HTML)
        vbox = soup.find("div", class_="votebox")
        parsed = parse_votebox(vbox)
        assert parsed[0]["name"] == "John Whitmire"
        assert parsed[1]["name"] == "Sheila Jackson Lee"

    def test_party_extraction(self) -> None:
        soup = _soup(VOTEBOX_HTML)
        vbox = soup.find("div", class_="votebox")
        parsed = parse_votebox(vbox)
        assert parsed[0]["party"] == "Nonpartisan"

    def test_vote_percentage(self) -> None:
        soup = _soup(VOTEBOX_HTML)
        vbox = soup.find("div", class_="votebox")
        parsed = parse_votebox(vbox)
        assert parsed[0]["vote_pct"] == 64.4
        assert parsed[1]["vote_pct"] == 35.6

    def test_ballotpedia_url(self) -> None:
        soup = _soup(VOTEBOX_HTML)
        vbox = soup.find("div", class_="votebox")
        parsed = parse_votebox(vbox)
        assert parsed[0]["wiki_url"] == f"{BALLOTPEDIA_BASE}/John_Whitmire"

    def test_empty_table(self) -> None:
        html = '<div class="votebox"><table></table></div>'
        soup = _soup(html)
        vbox = soup.find("div", class_="votebox")
        assert parse_votebox(vbox) == []


# ── TestParseRcvVotebox ───────────────────────────────────────────────────


class TestParseRcvVotebox:
    """Tests for parse_rcv_votebox()."""

    def test_parses_final_round(self) -> None:
        soup = _soup(RCV_VOTEBOX_HTML)
        vbox = soup.find("div", class_="rcvvotebox")
        parsed = parse_rcv_votebox(vbox)
        assert len(parsed) == 2

    def test_winner_from_final_round(self) -> None:
        soup = _soup(RCV_VOTEBOX_HTML)
        vbox = soup.find("div", class_="rcvvotebox")
        parsed = parse_rcv_votebox(vbox)
        assert parsed[0]["name"] == "Daniel Lurie"
        assert parsed[0]["is_winner"] is True
        assert parsed[0]["vote_pct"] == 55.0

    def test_loser_from_final_round(self) -> None:
        soup = _soup(RCV_VOTEBOX_HTML)
        vbox = soup.find("div", class_="rcvvotebox")
        parsed = parse_rcv_votebox(vbox)
        assert parsed[1]["name"] == "London Breed"
        assert parsed[1]["is_winner"] is False


# ── TestParseStatePage ────────────────────────────────────────────────────


class TestParseStatePageMultiStage:
    """Tests for parse_state_page() with multiple election stages."""

    def test_returns_two_stages(self) -> None:
        scraper = BallotpediaMunicipalScraper()
        soup = _soup(MULTI_STAGE_HTML)
        results = scraper.parse_state_page("Jacksonville, Florida", soup, 2023)
        assert len(results) == 2

    def test_stage_types(self) -> None:
        scraper = BallotpediaMunicipalScraper()
        soup = _soup(MULTI_STAGE_HTML)
        results = scraper.parse_state_page("Jacksonville, Florida", soup, 2023)
        stages = {r[0].election_stage for r in results}
        assert stages == {"general", "runoff"}

    def test_election_fields(self) -> None:
        scraper = BallotpediaMunicipalScraper()
        soup = _soup(MULTI_STAGE_HTML)
        results = scraper.parse_state_page("Jacksonville, Florida", soup, 2023)
        for election, _ in results:
            assert election.state == "Jacksonville, Florida"
            assert election.race_type == "Mayor"
            assert election.year == 2023


class TestParseStatePageEmpty:
    """Tests for parse_state_page() with no results."""

    def test_no_voteboxes(self) -> None:
        scraper = BallotpediaMunicipalScraper()
        soup = _soup("<html><body><p>No election data.</p></body></html>")
        results = scraper.parse_state_page("Phoenix, Arizona", soup, 2024)
        assert results == []


# ── TestCollectStateUrls ──────────────────────────────────────────────────


class TestCollectStateUrls:
    """Tests for collect_state_urls()."""

    def test_extracts_cities(self) -> None:
        scraper = BallotpediaMunicipalScraper()
        soup = _soup(TOP_100_TABLE_HTML)
        results = scraper.collect_state_urls(soup, 2023)
        assert len(results) == 3

    def test_city_state_format(self) -> None:
        scraper = BallotpediaMunicipalScraper()
        soup = _soup(TOP_100_TABLE_HTML)
        results = scraper.collect_state_urls(soup, 2023)
        assert results[0][0] == "New York, New York"
        assert results[1][0] == "Los Angeles, California"

    def test_url_construction(self) -> None:
        scraper = BallotpediaMunicipalScraper()
        soup = _soup(TOP_100_TABLE_HTML)
        results = scraper.collect_state_urls(soup, 2023)
        assert results[0][1] == (
            f"{BALLOTPEDIA_BASE}/Mayoral_election_in_New_York,_New_York_(2023)"
        )
        assert results[1][1] == (
            f"{BALLOTPEDIA_BASE}/Mayoral_election_in_Los_Angeles,_California_(2023)"
        )

    def test_preserves_periods_in_url(self) -> None:
        scraper = BallotpediaMunicipalScraper()
        soup = _soup(TOP_100_TABLE_HTML)
        results = scraper.collect_state_urls(soup, 2024)
        # St. Louis should preserve the period
        assert results[2][0] == "St. Louis, Missouri"
        assert "St._Louis" in results[2][1]

    def test_fallback_on_empty_table(self) -> None:
        scraper = BallotpediaMunicipalScraper()
        soup = _soup("<html><body></body></html>")
        results = scraper.collect_state_urls(soup, 2023)
        # Should use fallback list
        assert len(results) == 100


# ── TestBuildIndexUrl ─────────────────────────────────────────────────────


class TestBuildIndexUrl:
    """Tests for build_index_url()."""

    def test_returns_top_100_url(self) -> None:
        scraper = BallotpediaMunicipalScraper()
        assert scraper.build_index_url(2024) == TOP_100_URL
