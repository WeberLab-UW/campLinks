"""Tests for the GovernorScraper."""

from __future__ import annotations

from bs4 import BeautifulSoup

from camplinks.scrapers.governor import GovernorScraper


def _make_soup(html: str) -> BeautifulSoup:
    """Create a BeautifulSoup from raw HTML."""
    return BeautifulSoup(html, "html.parser")


class TestGovernorBuildIndexUrl:
    """Tests for GovernorScraper.build_index_url."""

    def test_url_format(self) -> None:
        """Index URL contains year and 'gubernatorial_elections'."""
        scraper = GovernorScraper()
        url = scraper.build_index_url(2025)
        assert "2025" in url
        assert "gubernatorial_elections" in url


class TestGovernorCollectStateUrls:
    """Tests for GovernorScraper.collect_state_urls."""

    def test_extracts_state_links(self) -> None:
        """Finds state gubernatorial election links."""
        html = """
        <html><body>
        <a href="/wiki/2025_Virginia_gubernatorial_election">Virginia</a>
        <a href="/wiki/2025_New_Jersey_gubernatorial_election">NJ</a>
        <a href="/wiki/2025_Virginia_gubernatorial_election">dup</a>
        </body></html>
        """
        scraper = GovernorScraper()
        results = scraper.collect_state_urls(_make_soup(html), 2025)
        assert len(results) == 2
        states = {r[0] for r in results}
        assert "Virginia" in states
        assert "New Jersey" in states

    def test_ignores_non_gubernatorial_links(self) -> None:
        """Skips links that don't match the gubernatorial pattern."""
        html = """
        <html><body>
        <a href="/wiki/2025_Virginia_attorney_general_election">AG</a>
        <a href="/wiki/2025_United_States_Senate_elections">Senate</a>
        </body></html>
        """
        scraper = GovernorScraper()
        results = scraper.collect_state_urls(_make_soup(html), 2025)
        assert len(results) == 0


class TestGovernorParseStatePage:
    """Tests for GovernorScraper.parse_state_page."""

    _STATE_HTML = """
    <html><body>
    <div class="mw-heading mw-heading3"><h3>General election</h3></div>
    <table class="wikitable plainrowheaders">
    <caption>General election results</caption>
    <tr><th>Party</th><th>Candidate</th><th>Votes</th><th>%</th></tr>
    <tr class="vcard">
      <td class="org">Democratic</td>
      <th class="fn"><b><a href="/wiki/Jane_Doe">Jane Doe</a></b></th>
      <td>1500000</td>
      <td>55.2</td>
    </tr>
    <tr class="vcard">
      <td class="org">Republican</td>
      <th class="fn"><a href="/wiki/John_Smith">John Smith</a></th>
      <td>1200000</td>
      <td>44.8</td>
    </tr>
    </table>
    </body></html>
    """

    def test_parses_candidates(self) -> None:
        """Extracts candidates from a general election table."""
        scraper = GovernorScraper()
        results = scraper.parse_state_page(
            "Virginia", _make_soup(self._STATE_HTML), 2025
        )
        assert len(results) == 1
        election, candidates = results[0]
        assert election.state == "Virginia"
        assert election.race_type == "Governor"
        assert election.year == 2025
        assert election.district is None
        assert len(candidates) == 2
        assert candidates[0].candidate_name == "Jane Doe"
        assert candidates[0].party == "Democratic"
        assert candidates[0].is_winner is True

    def test_empty_page_returns_empty(self) -> None:
        """Returns empty list for a page with no election tables."""
        html = "<html><body><p>No tables here.</p></body></html>"
        scraper = GovernorScraper()
        results = scraper.parse_state_page("Virginia", _make_soup(html), 2025)
        assert results == []
