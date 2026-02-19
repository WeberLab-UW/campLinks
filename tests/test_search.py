"""Unit tests for camplinks.search module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from bs4 import BeautifulSoup

from camplinks.cache import load_cache, make_cache_key, save_cache
from camplinks.search import (
    extract_all_contact_links,
    find_ballotpedia_url,
    score_campaign_url,
    search_campaign_site_web,
)


# ---------------------------------------------------------------------------
# Ballotpedia contact extraction
# ---------------------------------------------------------------------------
class TestExtractAllContactLinks:
    """Tests for Ballotpedia infobox contact link extraction."""

    def test_extracts_all_contact_types(self) -> None:
        html = """
        <div class="infobox person">
          <div class="widget-row value-only Republican">Contact</div>
          <div class="widget-row value-only white">
            <a href="https://example.com/campaign" target="_blank">
              Campaign website
            </a>
          </div>
          <div class="widget-row value-only white">
            <a href="https://facebook.com/campaign" target="_blank">
              Campaign Facebook
            </a>
          </div>
          <div class="widget-row value-only white">
            <a href="https://x.com/candidate" target="_blank">
              Campaign X
            </a>
          </div>
          <div class="widget-row value-only white">
            <a href="https://linkedin.com/in/person" target="_blank">
              Personal LinkedIn
            </a>
          </div>
          <div class="widget-row value-only Republican">Biography</div>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        links = extract_all_contact_links(soup)
        assert len(links) == 4
        assert links["campaign website"] == "https://example.com/campaign"
        assert links["campaign facebook"] == "https://facebook.com/campaign"
        assert links["campaign x"] == "https://x.com/candidate"
        assert links["personal linkedin"] == "https://linkedin.com/in/person"

    def test_stops_at_next_section_header(self) -> None:
        html = """
        <div class="infobox person">
          <div class="widget-row value-only Democrat">Contact</div>
          <div class="widget-row value-only white">
            <a href="https://example.com" target="_blank">Campaign website</a>
          </div>
          <div class="widget-row value-only Democrat">Biography</div>
          <div class="widget-row value-only white">
            <a href="https://other.com" target="_blank">Some other link</a>
          </div>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        links = extract_all_contact_links(soup)
        assert len(links) == 1
        assert "campaign website" in links

    def test_no_infobox_returns_empty(self) -> None:
        html = "<div><p>No infobox here</p></div>"
        soup = BeautifulSoup(html, "lxml")
        assert extract_all_contact_links(soup) == {}

    def test_no_contact_section_returns_empty(self) -> None:
        html = """
        <div class="infobox person">
          <div class="widget-row value-only Republican">Biography</div>
          <div class="widget-row value-only white">Some text</div>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        assert extract_all_contact_links(soup) == {}

    def test_contact_row_without_link_skipped(self) -> None:
        html = """
        <div class="infobox person">
          <div class="widget-row value-only Democrat">Contact</div>
          <div class="widget-row value-only white">Plain text no link</div>
          <div class="widget-row value-only white">
            <a href="https://example.com">Campaign website</a>
          </div>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        links = extract_all_contact_links(soup)
        assert len(links) == 1
        assert links["campaign website"] == "https://example.com"


# ---------------------------------------------------------------------------
# URL scoring heuristics
# ---------------------------------------------------------------------------
class TestScoreCampaignUrl:
    """Tests for campaign URL scoring."""

    def test_obvious_campaign_site_high_score(self) -> None:
        score = score_campaign_url(
            "https://www.smithforcongress.com/",
            "John Smith for Congress",
            "Official campaign website",
            "Smith",
            "Ohio",
        )
        assert score >= 0.5

    def test_news_domain_zero_score(self) -> None:
        assert (
            score_campaign_url(
                "https://www.nytimes.com/article",
                "Article about Smith",
                "News story",
                "Smith",
                "Ohio",
            )
            == 0.0
        )

    def test_ballotpedia_zero_score(self) -> None:
        assert (
            score_campaign_url(
                "https://ballotpedia.org/John_Smith",
                "John Smith - Ballotpedia",
                "Profile",
                "Smith",
                "Ohio",
            )
            == 0.0
        )

    def test_gov_domain_zero_score(self) -> None:
        assert (
            score_campaign_url(
                "https://smith.house.gov/",
                "Congressman Smith",
                "Official page",
                "Smith",
                "Ohio",
            )
            == 0.0
        )

    def test_social_media_zero_score(self) -> None:
        for domain in ("facebook.com", "x.com", "twitter.com", "instagram.com"):
            assert (
                score_campaign_url(
                    f"https://www.{domain}/smithforcongress",
                    "Smith Campaign",
                    "Social media page",
                    "Smith",
                    "Ohio",
                )
                == 0.0
            ), f"{domain} should score 0"

    def test_name_in_domain_boosts_score(self) -> None:
        with_name = score_campaign_url(
            "https://www.smithforamerica.com/",
            "Smith Campaign",
            "Website",
            "Smith",
            "Ohio",
        )
        without_name = score_campaign_url(
            "https://www.genericsite.com/",
            "Smith Campaign",
            "Website",
            "Smith",
            "Ohio",
        )
        assert with_name > without_name

    def test_score_capped_at_one(self) -> None:
        score = score_campaign_url(
            "https://www.smithforcongress.com/",
            "Official John Smith for Congress Ohio campaign",
            "Official campaign website for Smith in Ohio",
            "Smith",
            "Ohio",
        )
        assert score <= 1.0

    def test_senate_keyword_boosts_score(self) -> None:
        score = score_campaign_url(
            "https://www.smithforsenate.com/",
            "Smith for Senate",
            "Official campaign",
            "Smith",
            "Ohio",
        )
        assert score >= 0.5


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------
class TestCacheHelpers:
    """Tests for cache persistence functions."""

    def test_make_cache_key(self) -> None:
        key = make_cache_key("Republican", "Ohio", "5", "John Smith")
        assert key == "Republican|Ohio|5|John Smith"

    def test_load_cache_missing_file(self, tmp_path: pytest.TempPathFactory) -> None:
        result = load_cache(str(tmp_path / "nonexistent.json"))
        assert result == {}

    def test_save_and_load_cache(self, tmp_path: pytest.TempPathFactory) -> None:
        cache_path = str(tmp_path / "test_cache.json")
        cache = {
            "Republican|Ohio|5|John Smith": {
                "campaign website": "https://example.com",
            }
        }
        save_cache(cache, cache_path)
        loaded = load_cache(cache_path)
        assert loaded == cache


# ---------------------------------------------------------------------------
# find_ballotpedia_url (mocked)
# ---------------------------------------------------------------------------
class TestFindBallotpediaUrl:
    """Tests for Ballotpedia URL discovery via DDG search."""

    @patch("camplinks.search.ddg_search")
    def test_returns_ballotpedia_url(self, mock_ddg: MagicMock) -> None:
        mock_ddg.return_value = [
            {
                "href": "https://ballotpedia.org/John_Smith",
                "title": "John Smith - Ballotpedia",
                "body": "...",
            }
        ]
        result = find_ballotpedia_url("John Smith", "Ohio")
        assert result == "https://ballotpedia.org/John_Smith"

    @patch("camplinks.search.ddg_search")
    def test_skips_wiki_subpages(self, mock_ddg: MagicMock) -> None:
        mock_ddg.return_value = [
            {
                "href": "https://ballotpedia.org/wiki/Category:Elections",
                "title": "Elections",
                "body": "...",
            },
            {
                "href": "https://ballotpedia.org/John_Smith",
                "title": "John Smith",
                "body": "...",
            },
        ]
        result = find_ballotpedia_url("John Smith", "Ohio")
        assert result == "https://ballotpedia.org/John_Smith"

    @patch("camplinks.search.ddg_search")
    def test_returns_empty_on_no_results(self, mock_ddg: MagicMock) -> None:
        mock_ddg.return_value = []
        result = find_ballotpedia_url("Unknown Person", "Ohio")
        assert result == ""

    @patch("camplinks.search.ddg_search")
    def test_returns_empty_when_no_ballotpedia_match(self, mock_ddg: MagicMock) -> None:
        mock_ddg.return_value = [
            {
                "href": "https://example.com/john-smith",
                "title": "John Smith",
                "body": "...",
            }
        ]
        result = find_ballotpedia_url("John Smith", "Ohio")
        assert result == ""


# ---------------------------------------------------------------------------
# search_campaign_site_web (mocked)
# ---------------------------------------------------------------------------
class TestSearchCampaignSiteWeb:
    """Tests for Tier 2 web search fallback."""

    @patch("camplinks.search.ddg_search")
    def test_returns_highest_scoring_url(self, mock_ddg: MagicMock) -> None:
        mock_ddg.return_value = [
            {
                "href": "https://www.nytimes.com/article",
                "title": "News about Smith",
                "body": "Article",
            },
            {
                "href": "https://www.smithforcongress.com/",
                "title": "Smith for Congress",
                "body": "Official campaign",
            },
        ]
        result = search_campaign_site_web("John Smith", "Ohio", "5")
        assert result == "https://www.smithforcongress.com/"

    @patch("camplinks.search.ddg_search")
    def test_returns_empty_below_threshold(self, mock_ddg: MagicMock) -> None:
        mock_ddg.return_value = [
            {
                "href": "https://randomsite.com/something/deep/path",
                "title": "Unrelated page",
                "body": "Nothing relevant",
            },
        ]
        result = search_campaign_site_web("John Smith", "Ohio", "5")
        assert result == ""

    @patch("camplinks.search.ddg_search")
    def test_stops_early_on_high_confidence(self, mock_ddg: MagicMock) -> None:
        mock_ddg.return_value = [
            {
                "href": "https://www.smithforcongress.com/",
                "title": "Smith for Congress Ohio",
                "body": "Official campaign website",
            },
        ]
        search_campaign_site_web("John Smith", "Ohio", "5")
        assert mock_ddg.call_count == 1
