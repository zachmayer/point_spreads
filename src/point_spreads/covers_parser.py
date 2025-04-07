"""Parser for Covers.com NCAA basketball HTML data."""

from datetime import date, datetime
from pathlib import Path

import polars as pl
import requests
from diskcache import FanoutCache  # type: ignore
from lxml import html
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_random_exponential

# Create a cache with unlimited size in the project root directory
cache_dir = Path(__file__).parent.parent.parent / ".cache"
cache_dir.mkdir(exist_ok=True)
cache = FanoutCache(directory=str(cache_dir))


# Define the schema for game data - this can be imported by other modules
GAME_DATA_SCHEMA = {
    "game_date": pl.Date,
    "updated_date": pl.Date,
    "home_team": pl.Utf8,
    "away_team": pl.Utf8,
    "spread": pl.Utf8,
    "total": pl.Utf8,
}


class GameData(BaseModel):
    game_date: date
    updated_date: date
    home_team: str
    away_team: str
    spread: str
    total: str


# Define an empty DataFrame schema that matches the GameData model
def get_empty_dataframe() -> pl.DataFrame:
    """Create an empty DataFrame with the correct schema for game data."""
    return pl.DataFrame(schema=GAME_DATA_SCHEMA)


@retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(5))
@cache.memoize(typed=True, expire=60 * 60 * 24, tag="html")
def download_covers_html(game_date: date) -> str:
    """
    Download Covers.com HTML for a specific date.
    """
    date_str = game_date.strftime("%Y-%m-%d")
    url = f"https://www.covers.com/Sports/NCAAB/Matchups?selectedDate={date_str}"

    response = requests.get(url)
    response.raise_for_status()

    return response.text


def _parse_games(
    html_content: str,
    expected_date: date,
    displayed_date_xpath: str,
) -> pl.DataFrame:
    """
    Parse games from Covers.com HTML, handling both pre-game and post-game containers.

    Args:
        html_content: HTML content as a string.
        expected_date: Date of the games.
        displayed_date_xpath: XPath to extract the displayed date string from the page.

    Returns:
        DataFrame containing game information.
    """
    tree = html.fromstring(html_content)

    # Date Validation
    displayed_date_str = tree.xpath(f"string({displayed_date_xpath})").strip()
    month_day = displayed_date_str.split()
    month_abbr = month_day[0]
    month_num = datetime.strptime(month_abbr, "%b").month

    # Extract years from the navigation links
    iso_dates = tree.xpath('//a[contains(@href, "selectedDate=")]/@href')
    years = {int(date.split("selectedDate=")[1][:4]) for date in iso_dates}

    # Determine the correct year
    if len(years) == 1:
        year = list(years)[0]
    elif month_num == 12:
        year = min(years)
    elif month_num == 1:
        year = max(years)
    else:
        raise ValueError(f"Multiple years ({years}) for month {month_num}, expected only in Dec/Jan")

    # Validate the displayed date matches the expected date
    full_date_str = f"{displayed_date_str} {year}"
    displayed_date = datetime.strptime(full_date_str, "%b %d %Y").date()
    if displayed_date != expected_date:
        return get_empty_dataframe()

    # Unified container XPath that gets all game boxes
    containers = tree.xpath('//article[contains(@class, "gamebox")]')
    games: list[GameData] = []

    # Teams XPath is common for both container types
    teams_xpath = './/p[contains(@class, "gamebox-header")]/strong[@class="text-uppercase"]'

    for container in containers:
        # Determine container type - pregame or postgame
        container_class = container.get("class", "")
        is_postgame = "postgamebox" in container_class

        # Extract teams (common logic)
        teams_text = container.xpath(f"string({teams_xpath})").strip()
        if "@" not in teams_text:
            continue  # Skip containers without valid team info

        away_team_raw, home_team_raw = teams_text.split("@")
        away_team = away_team_raw.strip()
        home_team = home_team_raw.strip()

        # Spreads/totals are show differently pre- vs post-game
        if is_postgame:
            # Historical games: Extract from summary box
            summary_box = container.find('.//p[contains(@class, "summary-box")]')
            if summary_box is None:
                raise ValueError(f"Summary box not found for game involving {home_team} vs {away_team}")

            summary_text = summary_box.text_content().strip()
            strong_tags = summary_box.findall(".//strong")
            strong_count = len(strong_tags)
            has_zero_spread = "(zero spread)" in summary_text

            if strong_count not in (1, 2):
                raise ValueError(
                    f"Expected 1 or 2 <strong> tags, found {strong_count} in summary box for {home_team} vs {away_team}"
                )

            if strong_count == 1 and not has_zero_spread:
                raise ValueError(f"Found 1 strong tag but no 'zero spread' text for {home_team} vs {away_team}")

            if strong_count == 2 and has_zero_spread:
                raise ValueError(f"Found 2 strong tags with 'zero spread' text for {home_team} vs {away_team}")

            # Parse based on structure
            if has_zero_spread:
                spread = "0"
                total = strong_tags[0].text_content()
            else:
                spread = strong_tags[0].text_content()
                total = strong_tags[1].text_content()
        else:
            spread_xpath = './/span[contains(@class, "team-consensus")][2]/text()[normalize-space()]'
            total_xpath = './/span[contains(@class, "team-overunder")]'

            spread_element = container.find(spread_xpath)
            total_element = container.find(total_xpath)

            if spread_element is None or total_element is None:
                raise ValueError(f"Missing spread or total element for pregame {home_team} vs {away_team}")

            spread = spread_element.text_content()
            total = total_element.text_content()

        # Unified cleaning logic for both paths
        spread = spread.strip().upper()
        total = total.strip().lower().replace("o/u ", "").replace("under ", "").replace("over ", "")

        # Create game data
        game_data = GameData(
            game_date=expected_date,
            updated_date=date.today(),
            home_team=home_team,
            away_team=away_team,
            spread=spread,
            total=total,
        )
        games.append(game_data)

    if not games:
        return get_empty_dataframe()

    return pl.DataFrame([game.model_dump() for game in games])


def get_covers_games(game_date: date) -> pl.DataFrame:
    """
    Get games for a specific date from Covers.com.
    Uses a single parser that handles both historical and future games.
    """
    html_content = download_covers_html(game_date)

    # XPath for the displayed date on the page
    displayed_date_xpath = (
        "//div[@id='covers-CoversScoreboard-league-next-and-prev']"
        "/a[@class='navigation-anchor active isDailySport']"
        "/div[@class='date']"
    )

    # Single parser call - no branching based on date
    return _parse_games(
        html_content=html_content,
        expected_date=game_date,
        displayed_date_xpath=displayed_date_xpath,
    )


# A few simple tests of the parser
if __name__ == "__main__":
    past_date = date(2023, 3, 8)
    print(f"--- Parsing Historical Example ({past_date}) ---")
    historical_games_df = get_covers_games(past_date)
    print(historical_games_df)

    past_date = date(2023, 3, 18)
    print(f"--- Parsing Historical Example ({past_date}) ---")
    historical_games_df = get_covers_games(past_date)
    print(historical_games_df)

    past_date = date(2024, 11, 13)
    print(f"--- Parsing Historical Example ({past_date}) ---")
    historical_games_df = get_covers_games(past_date)
    print(historical_games_df)

    future_date = date(2025, 4, 6)
    print(f"\n--- Parsing Future Example ({future_date}) ---")
    future_games_df = get_covers_games(future_date)
    print(future_games_df)

    past_date = date(2020, 7, 4)
    print(f"--- Parsing Historical Example with no games ({past_date}) ---")
    historical_games_df = get_covers_games(past_date)
    print(historical_games_df)

    future_date = date(2025, 7, 4)
    print(f"\n--- Parsing Future Example with no games ({future_date}) ---")
    future_games_df = get_covers_games(future_date)
    print(future_games_df)
