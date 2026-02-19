"""Tests for the JudicialScraper."""

from __future__ import annotations

from bs4 import BeautifulSoup

from camplinks.scrapers.judicial import JudicialScraper, _parse_retention_table


def _make_soup(html: str) -> BeautifulSoup:
    """Create a BeautifulSoup from raw HTML."""
    return BeautifulSoup(html, "html.parser")


class TestJudicialCollectStateUrls:
    """Tests for JudicialScraper.collect_state_urls."""

    def test_extracts_supreme_court_links(self) -> None:
        """Finds state supreme court election links."""
        html = """
        <html><body>
        <a href="/wiki/2025_Wisconsin_Supreme_Court_election">WI</a>
        <a href="/wiki/2025_Louisiana_Supreme_Court_election">LA</a>
        <a href="/wiki/2025_Wisconsin_Supreme_Court_election">dup</a>
        </body></html>
        """
        scraper = JudicialScraper()
        results = scraper.collect_state_urls(_make_soup(html), 2025)
        assert len(results) == 2
        states = {r[0] for r in results}
        assert "Wisconsin" in states
        assert "Louisiana" in states


class TestJudicialParseStatePage:
    """Tests for JudicialScraper.parse_state_page (contested election)."""

    _CONTESTED_HTML = """
    <html><body>
    <div class="mw-heading mw-heading3"><h3>General election</h3></div>
    <table class="wikitable plainrowheaders">
    <caption>General election results</caption>
    <tr><th>Party</th><th>Candidate</th><th>Votes</th><th>%</th></tr>
    <tr class="vcard">
      <td class="org">Nonpartisan</td>
      <th class="fn"><b><a href="/wiki/Susan_Crawford">Susan Crawford</a></b></th>
      <td>800000</td>
      <td>55.5</td>
    </tr>
    <tr class="vcard">
      <td class="org">Nonpartisan</td>
      <th class="fn"><a href="/wiki/Brad_Schimel">Brad Schimel</a></th>
      <td>640000</td>
      <td>44.5</td>
    </tr>
    </table>
    </body></html>
    """

    def test_parses_contested_election(self) -> None:
        """Extracts candidates from a contested judicial election."""
        scraper = JudicialScraper()
        results = scraper.parse_state_page(
            "Wisconsin", _make_soup(self._CONTESTED_HTML), 2025
        )
        assert len(results) == 1
        election, candidates = results[0]
        assert election.race_type == "State Supreme Court"
        assert election.district is None
        assert len(candidates) == 2
        assert candidates[0].candidate_name == "Susan Crawford"
        assert candidates[0].is_winner is True


class TestParseRetentionTable:
    """Tests for _parse_retention_table."""

    _RETENTION_HTML = """
    <table class="wikitable">
    <tr><th>Justice</th><th>Yes</th><th>No</th></tr>
    <tr>
      <td><a href="/wiki/Christine_Donohue">Christine Donohue</a></td>
      <td><b>65%</b></td>
      <td>35%</td>
    </tr>
    </table>
    """

    def test_parses_retention_vote(self) -> None:
        """Extracts justice from a retention table."""
        soup = _make_soup(self._RETENTION_HTML)
        table = soup.find("table")
        results = _parse_retention_table(table)
        assert len(results) == 1
        assert results[0]["name"] == "Christine Donohue"
        assert results[0]["is_winner"] is True
