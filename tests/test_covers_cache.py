"""Tests for the on-disk HTML cache in front of Covers.com downloads."""

import os
import time
from datetime import date
from pathlib import Path

import pytest

from point_spreads import covers_parser

GAME_DATE = date(2024, 3, 8)


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


@pytest.fixture
def fetched_urls(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> list[str]:
    """Point the cache at a temp dir and record every URL the fake network serves."""
    urls: list[str] = []

    def fake_get(url: str) -> FakeResponse:
        urls.append(url)
        return FakeResponse(f"<html>fetch {len(urls)}</html>")

    monkeypatch.setattr(covers_parser, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(covers_parser.requests, "get", fake_get)
    return urls


def test_fresh_page_is_served_from_cache(fetched_urls: list[str], tmp_path: Path) -> None:
    first = covers_parser.download_covers_html(GAME_DATE)
    second = covers_parser.download_covers_html(GAME_DATE)

    assert first == second == "<html>fetch 1</html>"
    assert fetched_urls == ["https://www.covers.com/Sports/NCAAB/Matchups?selectedDate=2024-03-08"]
    assert [p.name for p in tmp_path.iterdir()] == ["2024-03-08.html"]


def test_page_older_than_a_day_is_fetched_again(fetched_urls: list[str], tmp_path: Path) -> None:
    covers_parser.download_covers_html(GAME_DATE)
    stale = time.time() - covers_parser.CACHE_TTL_SECONDS - 60
    os.utime(tmp_path / "2024-03-08.html", (stale, stale))

    assert covers_parser.download_covers_html(GAME_DATE) == "<html>fetch 2</html>"
    assert len(fetched_urls) == 2
