"""Tests for the AttorneyGeneralScraper."""

from __future__ import annotations

from bs4 import BeautifulSoup

from camplinks.scrapers.attorney_general import AttorneyGeneralScraper


def _make_soup(html: str) -> BeautifulSoup:
    """Create a BeautifulSoup from raw HTML."""
    return BeautifulSoup(html, "html.parser")


class TestAttorneyGeneralCollectStateUrls:
    """Tests for AttorneyGeneralScraper.collect_state_urls."""

    def test_extracts_ag_links(self) -> None:
        """Finds state attorney general election links."""
        html = """
        <html><body>
        <a href="/wiki/2025_Virginia_attorney_general_election">Virginia</a>
        <a href="/wiki/2025_Virginia_attorney_general_election">dup</a>
        </body></html>
        """
        scraper = AttorneyGeneralScraper()
        results = scraper.collect_state_urls(_make_soup(html), 2025)
        assert len(results) == 1
        assert results[0][0] == "Virginia"

    def test_ignores_non_ag_links(self) -> None:
        """Skips links that don't match AG pattern."""
        html = """
        <html><body>
        <a href="/wiki/2025_Virginia_gubernatorial_election">Gov</a>
        </body></html>
        """
        scraper = AttorneyGeneralScraper()
        results = scraper.collect_state_urls(_make_soup(html), 2025)
        assert len(results) == 0


class TestAttorneyGeneralParseStatePage:
    """Tests for AttorneyGeneralScraper.parse_state_page."""

    _PAGE_HTML = """
    <html><body>
    <div class="mw-heading mw-heading3"><h3>General election</h3></div>
    <table class="wikitable plainrowheaders">
    <caption>General election results</caption>
    <tr><th>Party</th><th>Candidate</th><th>Votes</th><th>%</th></tr>
    <tr class="vcard">
      <td class="org">Democratic</td>
      <th class="fn"><b><a href="/wiki/Jay_Jones">Jay Jones</a></b></th>
      <td>1800000</td>
      <td>54.1</td>
    </tr>
    <tr class="vcard">
      <td class="org">Republican</td>
      <th class="fn"><a href="/wiki/Jason_Miyares">Jason Miyares</a></th>
      <td>1500000</td>
      <td>45.9</td>
    </tr>
    </table>
    </body></html>
    """

    def test_parses_ag_election(self) -> None:
        """Extracts candidates from an AG election page."""
        scraper = AttorneyGeneralScraper()
        results = scraper.parse_state_page(
            "Virginia", _make_soup(self._PAGE_HTML), 2025
        )
        assert len(results) == 1
        election, candidates = results[0]
        assert election.race_type == "Attorney General"
        assert election.district is None
        assert len(candidates) == 2
        assert candidates[0].candidate_name == "Jay Jones"
        assert candidates[0].is_winner is True
