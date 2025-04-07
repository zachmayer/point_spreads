"""Parser for Covers.com NCAA basketball HTML data."""

import re
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
    container_xpath: str,
    teams_xpath: str,
    spread_xpath: str,
    total_xpath: str,
    displayed_date_xpath: str,
) -> pl.DataFrame:
    """
    Generic parser for Covers.com game data using provided XPaths.
    Includes validation against the displayed date on the page.

    Args:
        html_content: HTML content as a string.
        expected_date: Date of the games.
        container_xpath: XPath to select the list of game container elements.
        teams_xpath: XPath to extract the raw teams string (e.g., "Away @ Home")
                     from within a game container. Requires string() wrapper.
        spread_xpath: XPath to extract the raw spread string from within a
                      game container. Requires string() wrapper.
        total_xpath: XPath to extract the raw total string (e.g., "o/u 140.0",
                     "under 145.5") from within a game container. Requires
                     string() wrapper.
        displayed_date_xpath: XPath to extract the displayed date string from the page.

    Returns:
        DataFrame containing game information.
    """
    tree = html.fromstring(html_content)

    # Date Validation
    # If you ask for a date with no games, covers will return the closest date with games
    # This leads to duplicates, as multuiple "dates" will return the same day's games
    # So we need to check the actual date on the page from covers
    # 1. Extract the displayed date text from the page
    displayed_date_str = tree.xpath(f"string({displayed_date_xpath})").strip()
    month_day = displayed_date_str.split()
    month_abbr = month_day[0]
    month_num = datetime.strptime(month_abbr, "%b").month

    # 2. Extract years from the navigation links
    iso_dates = tree.xpath('//a[contains(@href, "selectedDate=")]/@href')
    years = {int(date.split("selectedDate=")[1][:4]) for date in iso_dates}

    # 3. Determine the correct year from the navigation links
    # None of these links are the current day: they show up to 3 days before/after today
    # Except on certain days in Dec/Jan these will all be the same year
    if len(years) == 1:  # Single year case (most common)
        year = list(years)[0]
    elif month_num == 12:  # December uses earlier year
        year = min(years)
    elif month_num == 1:  # January uses later year
        year = max(years)
    else:  # Unexpected case - should not happen
        raise ValueError(f"Multiple years ({years}) for month {month_num}, expected only in Dec/Jan")

    # 4. Construct full date and validate against expected
    full_date_str = f"{displayed_date_str} {year}"
    displayed_date = datetime.strptime(full_date_str, "%b %d %Y").date()
    if displayed_date != expected_date:
        return get_empty_dataframe()

    # Extract game data
    game_containers = tree.xpath(container_xpath)
    games: list[GameData] = []
    for container in game_containers:
        teams_text = container.xpath(f"string({teams_xpath})").strip()
        spread_text = container.xpath(f"string({spread_xpath})").strip().lower()
        total_text = container.xpath(f"string({total_xpath})").strip().lower()

        if "@" not in teams_text:
            raise ValueError(f"Missing '@' separator in teams text: '{teams_text}'")

        away_team_raw, home_team_raw = teams_text.split("@")
        away_team = away_team_raw.strip()
        home_team = home_team_raw.strip()

        # Extract just the spread value using regex to find first occurrence of + or - and everything after
        match = re.search(r"[+-].*", spread_text)
        spread = match.group(0).upper() if match else spread_text.upper()

        total_cleaned = total_text.lower().replace("o/u ", "")
        total_cleaned = total_cleaned.replace("under ", "").replace("over ", "")

        game_data = GameData(
            game_date=expected_date,
            updated_date=date.today(),
            home_team=home_team,
            away_team=away_team,
            spread=spread,
            total=total_cleaned,
        )
        games.append(game_data)

    if not games:
        return get_empty_dataframe()

    return pl.DataFrame([game.model_dump() for game in games])


def get_covers_games(game_date: date) -> pl.DataFrame:
    """
    Get games for a specific date from Covers.com.
    """
    try:
        html_content = download_covers_html(game_date)
        today = date.today()

        # XPath for the actual date on the page (may not match the requested date!)
        displayed_date_xpath = (
            "//div[@id='covers-CoversScoreboard-league-next-and-prev']"
            "/a[@class='navigation-anchor active isDailySport']"
            "/div[@class='date']"
        )

        # Assume for today we're parsing the morning before the games start
        # History pages vs future pages have a different format
        if game_date < today:
            # Historical games have a different structure
            container_xpath = '//article[contains(@class, "gamebox") and contains(@class, "postgamebox")]'
            teams_xpath = './/p[contains(@class, "gamebox-header")]/strong[@class="text-uppercase"]'

            # Spread is in a <strong> element contained within a paragraph with class "summary-box"
            # The text contains the team name + the spread value, e.g., "UNC -10.5"
            spread_xpath = './/p[contains(@class, "summary-box")]/strong[1]'

            # Total is in a <strong> tag in the summary box with text starting with "under" or "over"
            total_xpath = './/p[contains(@class, "summary-box")]/strong[starts-with(normalize-space(text()), "under ") or starts-with(normalize-space(text()), "over ")]'
        else:
            # Future games have a different structure
            container_xpath = '//article[contains(@class, "gamebox pregamebox")]'
            teams_xpath = './/p[@id="gamebox-header"]/strong[@class="text-uppercase"]'
            spread_xpath = './/span[contains(@class, "team-consensus")][2]/text()[normalize-space()]'
            total_xpath = './/span[contains(@class, "team-overunder")]'

        return _parse_games(
            html_content,
            expected_date=game_date,
            container_xpath=container_xpath,
            teams_xpath=teams_xpath,
            spread_xpath=spread_xpath,
            total_xpath=total_xpath,
            displayed_date_xpath=displayed_date_xpath,
        )
    except Exception as e:
        # Add date context to the exception and re-raise
        raise type(e)(f"Error for date {game_date}: {e}") from e


# A few simple tests of the parser
if __name__ == "__main__":
    past_date = date(2023, 3, 8)
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
