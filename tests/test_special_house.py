"""Tests for the SpecialHouseScraper."""

from __future__ import annotations

from bs4 import BeautifulSoup

from camplinks.scrapers.special_house import SpecialHouseScraper


def _make_soup(html: str) -> BeautifulSoup:
    """Create a BeautifulSoup from raw HTML."""
    return BeautifulSoup(html, "html.parser")


class TestSpecialHouseBuildIndexUrl:
    """Tests for SpecialHouseScraper.build_index_url."""

    def test_url_format(self) -> None:
        """Index URL matches the House elections page."""
        scraper = SpecialHouseScraper()
        url = scraper.build_index_url(2025)
        assert "2025" in url
        assert "House_of_Representatives_elections" in url


class TestSpecialHouseCollectStateUrls:
    """Tests for SpecialHouseScraper.collect_state_urls."""

    def test_extracts_special_election_links(self) -> None:
        """Finds links to individual special election pages."""
        html = """
        <html><body>
        <a href="/wiki/2025_Florida%27s_1st_congressional_district_special_election">FL-1</a>
        <a href="/wiki/2025_Virginia%27s_11th_congressional_district_special_election">VA-11</a>
        <a href="/wiki/2025_Florida%27s_1st_congressional_district_special_election">dup</a>
        </body></html>
        """
        scraper = SpecialHouseScraper()
        results = scraper.collect_state_urls(_make_soup(html), 2025)
        assert len(results) == 2
        states = {r[0] for r in results}
        assert "Florida" in states
        assert "Virginia" in states

    def test_ignores_regular_election_links(self) -> None:
        """Skips links to regular state election pages."""
        html = """
        <html><body>
        <a href="/wiki/2025_United_States_House_of_Representatives_elections_in_Alabama">AL</a>
        <a href="/wiki/2025_United_States_gubernatorial_elections">Gov</a>
        </body></html>
        """
        scraper = SpecialHouseScraper()
        results = scraper.collect_state_urls(_make_soup(html), 2025)
        assert len(results) == 0


class TestSpecialHouseParseStatePage:
    """Tests for SpecialHouseScraper.parse_state_page."""

    _PAGE_HTML = """
    <html><head><title>2025 Florida's 1st congressional district special
    election - Wikipedia</title></head><body>
    <div class="mw-heading mw-heading3"><h3>General election</h3></div>
    <table class="wikitable plainrowheaders">
    <caption>General election results</caption>
    <tr><th>Party</th><th>Candidate</th><th>Votes</th><th>%</th></tr>
    <tr class="vcard">
      <td class="org">Republican</td>
      <th class="fn"><b><a href="/wiki/Jimmy_Patronis">Jimmy Patronis</a></b></th>
      <td>85000</td>
      <td>57.3</td>
    </tr>
    <tr class="vcard">
      <td class="org">Democratic</td>
      <th class="fn"><a href="/wiki/Gay_Valimont">Gay Valimont</a></th>
      <td>63000</td>
      <td>42.7</td>
    </tr>
    </table>
    </body></html>
    """

    def test_parses_special_election(self) -> None:
        """Extracts candidates from a special election page."""
        scraper = SpecialHouseScraper()
        results = scraper.parse_state_page("Florida", _make_soup(self._PAGE_HTML), 2025)
        assert len(results) == 1
        election, candidates = results[0]
        assert election.state == "Florida"
        assert election.race_type == "US House"
        assert election.year == 2025
        assert election.district == "1"
        assert len(candidates) == 2
        assert candidates[0].candidate_name == "Jimmy Patronis"
        assert candidates[0].is_winner is True

    def test_empty_page_returns_empty(self) -> None:
        """Returns empty list for a page with no results."""
        html = "<html><head><title>Test</title></head><body></body></html>"
        scraper = SpecialHouseScraper()
        results = scraper.parse_state_page("Florida", _make_soup(html), 2025)
        assert results == []
