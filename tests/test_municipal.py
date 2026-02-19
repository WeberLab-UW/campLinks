"""Tests for the MunicipalScraper."""

from __future__ import annotations

from bs4 import BeautifulSoup

from camplinks.scrapers.municipal import (
    MunicipalScraper,
    _extract_city_name,
    _is_results_table,
)


def _make_soup(html: str) -> BeautifulSoup:
    """Create a BeautifulSoup from raw HTML."""
    return BeautifulSoup(html, "html.parser")


class TestExtractCityName:
    """Tests for _extract_city_name."""

    def test_simple_city(self) -> None:
        """Extracts city name from standard title."""
        assert _extract_city_name("2025 Boston mayoral election", 2025) == "Boston"

    def test_city_with_state(self) -> None:
        """Extracts city with state qualifier."""
        assert (
            _extract_city_name("2025 Toledo, Ohio mayoral election", 2025)
            == "Toledo, Ohio"
        )

    def test_municipal_election(self) -> None:
        """Handles 'municipal election' suffix."""
        assert (
            _extract_city_name("2025 Madison, Alabama municipal election", 2025)
            == "Madison, Alabama"
        )


class TestIsResultsTable:
    """Tests for _is_results_table."""

    def test_results_table(self) -> None:
        """Identifies a table with vote headers."""
        html = """
        <table class="wikitable">
        <tr><th>Candidate</th><th>Votes</th><th>%</th></tr>
        </table>
        """
        soup = _make_soup(html)
        assert _is_results_table(soup.find("table")) is True

    def test_non_results_table(self) -> None:
        """Rejects a table without vote headers."""
        html = """
        <table class="wikitable">
        <tr><th>Name</th><th>Date</th></tr>
        </table>
        """
        soup = _make_soup(html)
        assert _is_results_table(soup.find("table")) is False


class TestMunicipalCollectStateUrls:
    """Tests for MunicipalScraper.collect_state_urls."""

    def test_extracts_from_category(self) -> None:
        """Finds mayoral election links from a category page."""
        html = """
        <html><body>
        <div class="mw-category">
        <ul>
          <li><a href="/wiki/2025_Boston_mayoral_election">2025 Boston mayoral election</a></li>
          <li><a href="/wiki/2025_Hoboken_mayoral_election">2025 Hoboken mayoral election</a></li>
        </ul>
        </div>
        </body></html>
        """
        scraper = MunicipalScraper()
        results = scraper.collect_state_urls(_make_soup(html), 2025)
        assert len(results) == 2
        cities = {r[0] for r in results}
        assert "Boston" in cities
        assert "Hoboken" in cities


class TestMunicipalParsePatternB:
    """Tests for Pattern B parsing (wikitable plainrowheaders + vcard)."""

    _PATTERN_B_HTML = """
    <html><body>
    <table class="wikitable plainrowheaders">
    <tr><th colspan="2">Party</th><th>Candidate</th><th>Votes</th><th>%</th></tr>
    <tr class="vcard">
      <td style="background-color:#C0C0C0;width:5px"></td>
      <td class="org"><b>Nonpartisan</b></td>
      <th scope="row" class="fn"><b><a href="/wiki/Emily_Jabbour">Emily Jabbour</a></b></th>
      <td style="text-align:right"><b>5170</b></td>
      <td style="text-align:right"><b>27.0%</b></td>
    </tr>
    <tr class="vcard">
      <td style="background-color:#C0C0C0;width:5px"></td>
      <td class="org"><b>Nonpartisan</b></td>
      <th scope="row" class="fn"><a href="/wiki/Michael_Russo">Michael Russo</a></th>
      <td style="text-align:right">4659</td>
      <td style="text-align:right">24.3%</td>
    </tr>
    </table>
    </body></html>
    """

    def test_parses_vcard_pattern(self) -> None:
        """Parses Pattern B (plainrowheaders + vcard) correctly."""
        scraper = MunicipalScraper()
        results = scraper.parse_state_page(
            "Hoboken", _make_soup(self._PATTERN_B_HTML), 2025
        )
        assert len(results) == 1
        election, candidates = results[0]
        assert election.state == "Hoboken"
        assert election.race_type == "Mayor"
        assert election.district is None
        assert len(candidates) == 2
        assert candidates[0].candidate_name == "Emily Jabbour"
        assert candidates[0].is_winner is True


class TestMunicipalParsePatternA:
    """Tests for Pattern A parsing (basic wikitable, no vcard)."""

    _PATTERN_A_HTML = """
    <html><body>
    <table class="wikitable">
    <tr><th colspan="2">Candidate</th><th>Votes</th><th>%</th></tr>
    <tr>
      <th scope="row" colspan="2"><b>Michelle Wu</b> (incumbent)</th>
      <td style="text-align:right;font-weight:bold;">66859</td>
      <td style="text-align:right;font-weight:bold;">71.85</td>
    </tr>
    <tr>
      <th scope="row" colspan="2">John Flaherty</th>
      <td style="text-align:right">26200</td>
      <td style="text-align:right">28.15</td>
    </tr>
    </table>
    </body></html>
    """

    def test_parses_basic_wikitable(self) -> None:
        """Parses Pattern A (basic wikitable, no vcard) correctly."""
        scraper = MunicipalScraper()
        results = scraper.parse_state_page(
            "Boston", _make_soup(self._PATTERN_A_HTML), 2025
        )
        assert len(results) == 1
        election, candidates = results[0]
        assert election.state == "Boston"
        assert election.race_type == "Mayor"
        assert len(candidates) == 2
        assert candidates[0].candidate_name == "Michelle Wu"
        assert candidates[0].is_winner is True

    def test_empty_page_returns_empty(self) -> None:
        """Returns empty list for a page with no tables."""
        html = "<html><body><p>No tables here.</p></body></html>"
        scraper = MunicipalScraper()
        results = scraper.parse_state_page("Boston", _make_soup(html), 2025)
        assert results == []
