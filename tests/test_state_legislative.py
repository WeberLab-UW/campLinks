"""Tests for the StateLegislativeScraper."""

from __future__ import annotations

from bs4 import BeautifulSoup

from camplinks.scrapers.state_legislative import (
    StateLegislativeScraper,
    _classify_chamber,
)


def _make_soup(html: str) -> BeautifulSoup:
    """Create a BeautifulSoup from raw HTML."""
    return BeautifulSoup(html, "html.parser")


class TestClassifyChamber:
    """Tests for _classify_chamber."""

    def test_lower_chamber_names(self) -> None:
        """Identifies lower chambers."""
        assert _classify_chamber("House of Delegates") == "State House"
        assert _classify_chamber("General Assembly") == "State House"
        assert _classify_chamber("House_of_Representatives") == "State House"

    def test_upper_chamber_names(self) -> None:
        """Identifies upper chambers."""
        assert _classify_chamber("State Senate") == "State Senate"
        assert _classify_chamber("senate election") == "State Senate"


class TestStateLegCollectStateUrls:
    """Tests for StateLegislativeScraper.collect_state_urls."""

    def test_extracts_chamber_links(self) -> None:
        """Finds links to state legislative election pages."""
        html = """
        <html><body>
        <a href="/wiki/2025_Virginia_House_of_Delegates_election">VA House</a>
        <a href="/wiki/2025_New_Jersey_General_Assembly_election">NJ Assembly</a>
        </body></html>
        """
        scraper = StateLegislativeScraper()
        results = scraper.collect_state_urls(_make_soup(html), 2025)
        assert len(results) == 2

    def test_skips_special_elections(self) -> None:
        """Skips links containing 'special'."""
        html = """
        <html><body>
        <a href="/wiki/2025_Georgia_House_of_Representatives_District_121_special_election">GA</a>
        </body></html>
        """
        scraper = StateLegislativeScraper()
        results = scraper.collect_state_urls(_make_soup(html), 2025)
        assert len(results) == 0


class TestStateLegParseStatePage:
    """Tests for StateLegislativeScraper.parse_state_page."""

    _PAGE_HTML = """
    <html><head><title>2025 Virginia House of Delegates election</title></head>
    <body>
    <div class="mw-heading mw-heading2"><h2>District 1</h2></div>
    <div class="mw-heading mw-heading3"><h3>General election</h3></div>
    <table class="wikitable plainrowheaders">
    <caption>General election</caption>
    <tr><th>Party</th><th>Candidate</th><th>Votes</th><th>%</th></tr>
    <tr class="vcard">
      <td class="org">Democratic</td>
      <th class="fn"><b><a href="/wiki/Alice_Jones">Alice Jones</a></b></th>
      <td>25000</td>
      <td>52.1</td>
    </tr>
    <tr class="vcard">
      <td class="org">Republican</td>
      <th class="fn"><a href="/wiki/Bob_Smith">Bob Smith</a></th>
      <td>23000</td>
      <td>47.9</td>
    </tr>
    </table>

    <div class="mw-heading mw-heading2"><h2>District 2</h2></div>
    <div class="mw-heading mw-heading3"><h3>General election</h3></div>
    <table class="wikitable plainrowheaders">
    <caption>General election</caption>
    <tr><th>Party</th><th>Candidate</th><th>Votes</th><th>%</th></tr>
    <tr class="vcard">
      <td class="org">Republican</td>
      <th class="fn"><b><a href="/wiki/Carol_White">Carol White</a></b></th>
      <td>28000</td>
      <td>54.3</td>
    </tr>
    <tr class="vcard">
      <td class="org">Democratic</td>
      <th class="fn"><a href="/wiki/Dan_Brown">Dan Brown</a></th>
      <td>24000</td>
      <td>45.7</td>
    </tr>
    </table>
    </body></html>
    """

    def test_parses_multiple_districts(self) -> None:
        """Extracts elections from multiple districts."""
        scraper = StateLegislativeScraper()
        results = scraper.parse_state_page(
            "Virginia", _make_soup(self._PAGE_HTML), 2025
        )
        assert len(results) == 2
        assert results[0][0].district == "1"
        assert results[1][0].district == "2"
        assert results[0][0].race_type == "State House"

    def test_extracts_candidates_per_district(self) -> None:
        """Each district has the correct candidates."""
        scraper = StateLegislativeScraper()
        results = scraper.parse_state_page(
            "Virginia", _make_soup(self._PAGE_HTML), 2025
        )
        _, cands_d1 = results[0]
        assert len(cands_d1) == 2
        assert cands_d1[0].candidate_name == "Alice Jones"
        assert cands_d1[0].is_winner is True
