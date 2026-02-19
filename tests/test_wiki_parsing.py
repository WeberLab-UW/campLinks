"""Unit tests for camplinks.wiki_parsing shared utilities."""

from __future__ import annotations

from bs4 import BeautifulSoup

from camplinks.wiki_parsing import (
    extract_district_number,
    find_preceding_heading,
    is_general_election_table,
    parse_candidate_row,
    to_float,
)


# ---------------------------------------------------------------------------
# to_float
# ---------------------------------------------------------------------------
class TestToFloat:
    """Tests for to_float helper."""

    def test_none_returns_none(self) -> None:
        assert to_float(None) is None

    def test_float_passthrough(self) -> None:
        assert to_float(54.6) == 54.6

    def test_string_conversion(self) -> None:
        assert to_float("78.4") == 78.4

    def test_invalid_string_returns_none(self) -> None:
        assert to_float("N/A") is None


# ---------------------------------------------------------------------------
# extract_district_number
# ---------------------------------------------------------------------------
class TestExtractDistrictNumber:
    """Tests for district number extraction from heading text."""

    def test_ordinal_suffix(self) -> None:
        assert extract_district_number("District 3[edit]") == "3"

    def test_first_district(self) -> None:
        assert extract_district_number("1st congressional district") == "1"

    def test_second_district(self) -> None:
        assert extract_district_number("2nd congressional district") == "2"

    def test_third_district(self) -> None:
        assert extract_district_number("23rd congressional district") == "23"

    def test_at_large(self) -> None:
        assert extract_district_number("At-large district") == "At-Large"

    def test_general_election_fallback(self) -> None:
        assert extract_district_number("General election[edit]") == "At-Large"


# ---------------------------------------------------------------------------
# find_preceding_heading
# ---------------------------------------------------------------------------
class TestFindPrecedingHeading:
    """Tests for heading traversal in Wikipedia HTML."""

    def test_finds_bare_h2(self) -> None:
        html = "<div><h2>District 1</h2><p>text</p><table id='t'></table></div>"
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        assert table is not None
        result = find_preceding_heading(table, ("h2",))
        assert result is not None
        assert result.get_text(strip=True) == "District 1"

    def test_finds_mw_heading_wrapper(self) -> None:
        html = (
            '<div><div class="mw-heading mw-heading2"><h2>District 5</h2></div>'
            "<p>text</p><table id='t'></table></div>"
        )
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        assert table is not None
        result = find_preceding_heading(table, ("h2",))
        assert result is not None
        assert result.get_text(strip=True) == "District 5"

    def test_returns_none_when_no_heading(self) -> None:
        html = "<div><p>no heading</p><table id='t'></table></div>"
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        assert table is not None
        assert find_preceding_heading(table, ("h2",)) is None


# ---------------------------------------------------------------------------
# is_general_election_table
# ---------------------------------------------------------------------------
class TestIsGeneralElectionTable:
    """Tests for general election table heuristic."""

    def test_caption_with_election(self) -> None:
        html = (
            '<table class="wikitable plainrowheaders">'
            "<caption>2024 Alabama's 1st congressional district election</caption>"
            "<tr><td></td></tr></table>"
        )
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        assert table is not None
        assert is_general_election_table(table) is True

    def test_caption_with_primary(self) -> None:
        html = (
            '<table class="wikitable plainrowheaders">'
            "<caption>Republican primary results</caption>"
            "<tr><td></td></tr></table>"
        )
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        assert table is not None
        assert is_general_election_table(table) is False

    def test_caption_with_runoff(self) -> None:
        html = (
            '<table class="wikitable plainrowheaders">'
            "<caption>Democratic primary runoff results</caption>"
            "<tr><td></td></tr></table>"
        )
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        assert table is not None
        assert is_general_election_table(table) is False


# ---------------------------------------------------------------------------
# parse_candidate_row
# ---------------------------------------------------------------------------
class TestParseCandidateRow:
    """Tests for candidate row extraction."""

    def test_standard_row(self) -> None:
        html = (
            '<tr class="vcard">'
            '<td style="background-color:#E81B23;width:5px"></td>'
            '<td class="org"><b><a href="/wiki/Republican_Party_(United_States)">'
            "Republican</a></b></td>"
            '<th class="fn" scope="row" style="text-align:left;font-weight:normal">'
            '<b><a href="/wiki/Barry_Moore_(American_politician)">Barry Moore</a></b></th>'
            '<td style="text-align:right"><b>166,488</b></td>'
            '<td style="text-align:right"><b>78.4</b></td>'
            "</tr>"
        )
        soup = BeautifulSoup(html, "lxml")
        row = soup.find("tr")
        assert row is not None
        result = parse_candidate_row(row)
        assert result is not None
        assert result["party"] == "Republican"
        assert result["name"] == "Barry Moore"
        assert (
            result["wiki_url"]
            == "https://en.wikipedia.org/wiki/Barry_Moore_(American_politician)"
        )
        assert result["vote_pct"] == 78.4
        assert result["is_winner"] is True

    def test_loser_row_no_bold(self) -> None:
        html = (
            '<tr class="vcard">'
            '<td style="background-color:#3333FF;width:2px"></td>'
            '<td class="org"><a href="/wiki/Democratic_Party_(United_States)">'
            "Democratic</a></td>"
            '<th class="fn" scope="row" style="text-align:left;font-weight:normal">'
            "Tom Holmes</th>"
            '<td style="text-align:right">45,611</td>'
            '<td style="text-align:right">21.5</td>'
            "</tr>"
        )
        soup = BeautifulSoup(html, "lxml")
        row = soup.find("tr")
        assert row is not None
        result = parse_candidate_row(row)
        assert result is not None
        assert result["party"] == "Democratic"
        assert result["name"] == "Tom Holmes"
        assert result["wiki_url"] == ""
        assert result["vote_pct"] == 21.5
        assert result["is_winner"] is False

    def test_incumbent_stripped(self) -> None:
        html = (
            '<tr class="vcard">'
            '<td style="background-color:#E81B23;width:5px"></td>'
            '<td class="org"><b>Republican</b></td>'
            '<th class="fn" scope="row"><b>'
            '<a href="/wiki/Somebody">Somebody</a>(incumbent)</b></th>'
            "<td><b>100,000</b></td>"
            "<td><b>60.0</b></td>"
            "</tr>"
        )
        soup = BeautifulSoup(html, "lxml")
        row = soup.find("tr")
        assert row is not None
        result = parse_candidate_row(row)
        assert result is not None
        assert result["name"] == "Somebody"

    def test_percentage_with_percent_sign(self) -> None:
        html = (
            '<tr class="vcard">'
            '<td style="background-color:#E81B23;width:5px"></td>'
            '<td class="org"><b>Republican</b></td>'
            '<th class="fn" scope="row"><b>Candidate</b></th>'
            "<td><b>184,680</b></td>"
            "<td><b>70.61%</b></td>"
            '<td style="text-align:right">+2.43</td>'
            "</tr>"
        )
        soup = BeautifulSoup(html, "lxml")
        row = soup.find("tr")
        assert row is not None
        result = parse_candidate_row(row)
        assert result is not None
        assert result["vote_pct"] == 70.61
