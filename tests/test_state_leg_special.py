"""Tests for the StateLegSpecialScraper."""

from __future__ import annotations

from bs4 import BeautifulSoup

from camplinks.scrapers.state_leg_special import (
    StateLegSpecialScraper,
    _classify_from_url,
    _extract_state_from_url,
)


def _make_soup(html: str) -> BeautifulSoup:
    """Create a BeautifulSoup from raw HTML."""
    return BeautifulSoup(html, "html.parser")


class TestClassifyFromUrl:
    """Tests for _classify_from_url."""

    def test_senate_url(self) -> None:
        """Identifies Senate from URL."""
        assert (
            _classify_from_url("2025_Iowa_Senate_District_1_special_election")
            == "State Senate"
        )

    def test_house_url(self) -> None:
        """Identifies House from URL."""
        assert (
            _classify_from_url("2025_Georgia_House_of_Representatives_District_121")
            == "State House"
        )


class TestExtractStateFromUrl:
    """Tests for _extract_state_from_url."""

    def test_georgia_house(self) -> None:
        """Extracts state from Georgia House URL."""
        href = (
            "/wiki/2025_Georgia_House_of_Representatives_District_121_special_election"
        )
        assert _extract_state_from_url(href, 2025) == "Georgia"

    def test_iowa_senate(self) -> None:
        """Extracts state from Iowa Senate URL."""
        href = "/wiki/2025_Iowa_Senate_District_1_special_election"
        assert _extract_state_from_url(href, 2025) == "Iowa"


class TestStateLegSpecialCollectStateUrls:
    """Tests for StateLegSpecialScraper.collect_state_urls."""

    def test_extracts_special_links(self) -> None:
        """Finds state legislative special election links."""
        html = """
        <html><body>
        <a href="/wiki/2025_Georgia_House_of_Representatives_District_121_special_election">GA</a>
        <a href="/wiki/2025_Iowa_Senate_District_1_special_election">IA</a>
        </body></html>
        """
        scraper = StateLegSpecialScraper()
        results = scraper.collect_state_urls(_make_soup(html), 2025)
        assert len(results) == 2

    def test_skips_congressional_specials(self) -> None:
        """Skips federal congressional special elections."""
        html = """
        <html><body>
        <a href="/wiki/2025_Florida%27s_1st_congressional_district_special_election">FL</a>
        </body></html>
        """
        scraper = StateLegSpecialScraper()
        results = scraper.collect_state_urls(_make_soup(html), 2025)
        assert len(results) == 0

    def test_skips_non_legislative(self) -> None:
        """Skips gubernatorial and other non-legislative specials."""
        html = """
        <html><body>
        <a href="/wiki/2025_Virginia_gubernatorial_special_election">Gov</a>
        <a href="/wiki/2025_Wisconsin_Supreme_Court_special_election">Court</a>
        </body></html>
        """
        scraper = StateLegSpecialScraper()
        results = scraper.collect_state_urls(_make_soup(html), 2025)
        assert len(results) == 0


class TestStateLegSpecialParseStatePage:
    """Tests for StateLegSpecialScraper.parse_state_page."""

    _PAGE_HTML = """
    <html><head><title>2025 Georgia House of Representatives District 121
    special election</title></head><body>
    <div class="mw-heading mw-heading3"><h3>General election</h3></div>
    <table class="wikitable plainrowheaders">
    <caption>General election</caption>
    <tr><th>Party</th><th>Candidate</th><th>Votes</th><th>%</th></tr>
    <tr class="vcard">
      <td class="org">Democratic</td>
      <th class="fn"><b><a href="/wiki/Tangie_Herring">Tangie Herring</a></b></th>
      <td>5000</td>
      <td>62.0</td>
    </tr>
    <tr class="vcard">
      <td class="org">Republican</td>
      <th class="fn"><a href="/wiki/Roy_Other">Roy Other</a></th>
      <td>3100</td>
      <td>38.0</td>
    </tr>
    </table>
    </body></html>
    """

    def test_parses_special_election(self) -> None:
        """Extracts candidates from a state leg special election."""
        scraper = StateLegSpecialScraper()
        results = scraper.parse_state_page("Georgia", _make_soup(self._PAGE_HTML), 2025)
        assert len(results) == 1
        election, candidates = results[0]
        assert election.state == "Georgia"
        assert election.race_type == "State House"
        assert election.district == "121"
        assert len(candidates) == 2
