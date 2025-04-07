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
) -> pl.DataFrame:
    """
    Parse games from Covers.com HTML, handling both pre-game and post-game containers.

    Args:
        html_content: HTML content as a string.
        expected_date: Date of the games.

    Returns:
        DataFrame containing game information.
    """
    tree = html.fromstring(html_content)

    # Date Validation
    displayed_date_xpath = (
        "//div[@id='covers-CoversScoreboard-league-next-and-prev']"
        "/a[@class='navigation-anchor active isDailySport']"
        "/div[@class='date']"
    )

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
        container_class = container.get("class", "")
        is_postgame = "postgamebox" in container_class
        is_historical = expected_date < date.today()

        # Sanity check: future games should not have post-game boxes
        if not is_historical and is_postgame:
            raise ValueError(f"Future game on {expected_date} has a post-game box, which should be impossible")

        teams_text = container.xpath(f"string({teams_xpath})").strip()
        if "@" not in teams_text:
            continue  # Skip containers without valid team info

        away_team_raw, home_team_raw = teams_text.split("@")
        away_team = away_team_raw.strip()
        home_team = home_team_raw.strip()

        # Use different XPaths based on container type
        if is_postgame or is_historical:
            # Check if the game was canceled (both teams have a dash "-" for their score)
            score_xpath = './/div[contains(@class, "gamebox-score")]'
            away_score = container.xpath(f"{score_xpath}[1]//text()")
            home_score = container.xpath(f"{score_xpath}[2]//text()")

            # Game canceled if both scores exist and are "-"
            is_canceled = away_score and away_score[0].strip() == "-" and home_score and home_score[0].strip() == "-"

            # Historical games: Extract from summary box
            summary_box = container.find('.//p[@class="m-0 summary-box border rounded py-2 pe-2 ps-5"]')

            # Handle cases where we use empty strings for spread and total:
            # 1. Game was canceled (both scores are "-")
            # 2. "Bets off" case (no summary box)
            if is_canceled or summary_box is None:
                # Game was canceled or bets are off - use empty strings for spread and total
                spread = ""
                total = ""
            else:
                # Normal case - extract from summary box text
                summary_text = summary_box.text_content().strip()

                # First check for push case
                if "pushed the spread" in summary_text.lower():
                    # Extract spread value for push
                    strong_tags = summary_box.findall(".//strong")
                    if strong_tags and len(strong_tags) >= 1:
                        spread = strong_tags[0].text_content()
                    else:
                        spread = "0"  # Default for push if no value found
                else:
                    # Regular case - find which team covered the spread
                    # Extract the relevant team name from the summary text
                    away_pattern = re.escape(away_team)
                    home_pattern = re.escape(home_team)
                    team_pattern = f"({away_pattern}|{home_pattern})"

                    team_covered_match = re.search(f"{team_pattern}\\s+covered the spread", summary_text)

                    # Extract spread value
                    strong_tags = summary_box.findall(".//strong")
                    if strong_tags and len(strong_tags) >= 1:
                        spread_value = strong_tags[0].text_content()
                    else:
                        spread_value = ""

                    if team_covered_match:
                        team_covered = team_covered_match.group(1)

                        # Determine sign based on which team covered
                        if team_covered == home_team:
                            # Home team covered - use spread as is
                            spread = spread_value
                        else:
                            # Away team covered - negate for home team perspective
                            if spread_value.startswith("+"):
                                spread = "-" + spread_value[1:]
                            elif spread_value.startswith("-"):
                                spread = "+" + spread_value[1:]
                            else:
                                spread = "-" + spread_value
                    else:
                        # Couldn't find which team covered, use the fallback method
                        if strong_tags and len(strong_tags) >= 1:
                            spread = strong_tags[0].text_content()
                        else:
                            spread = "0"  # Default for push if no value found

                # Extract total from strong tags if available
                strong_tags = summary_box.findall(".//strong")
                if len(strong_tags) >= 2:
                    # For normal games, total is the second strong tag
                    total = strong_tags[1].text_content()
                elif len(strong_tags) >= 3:
                    # For pushed games, total is the third strong tag
                    total = strong_tags[2].text_content()
                else:
                    # Try to extract from text patterns if not found in strong tags
                    total_match = None
                    summary_html = html.tostring(summary_box, encoding="unicode")

                    if "under" in summary_text.lower():
                        total_match = re.search(r"under\s+<strong>([0-9.]+)</strong>", summary_html)
                    elif "over" in summary_text.lower():
                        total_match = re.search(r"over\s+<strong>([0-9.]+)</strong>", summary_html)
                    elif "pre-game total" in summary_text.lower():
                        total_match = re.search(r"pre-game total of\s+<strong>([0-9.]+)</strong>", summary_html)

                    if total_match:
                        total = total_match.group(1)
                    else:
                        total = ""
        else:
            # Use simpler selectors for future games and manually select the right elements
            # For spread, get all team-consensus spans and take the last one (home team)
            spread_elements = container.xpath('.//span[contains(@class, "team-consensus")]')
            total_element = container.xpath('.//span[contains(@class, "team-overunder")]')

            if not spread_elements or not total_element:
                raise ValueError(f"Missing elements: {home_team} vs {away_team} on {expected_date}")

            # The second team-consensus span is for the home team (right side)
            if len(spread_elements) >= 2:
                spread_element = spread_elements[1]  # Home team (right side) spread
                # Get only the text node content without any child elements
                spread_text = spread_element.text or ""
                # Clean and extract just the numeric part with sign
                spread = spread_text.strip()
            else:
                raise ValueError(f"Insufficient spread elements: {home_team} vs {away_team} on {expected_date}")

            total = total_element[0].text_content()

        # Unified cleaning logic for both paths - only clean if not empty
        if spread:
            spread = spread.strip().upper()
        if total:
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

    return _parse_games(
        html_content=html_content,
        expected_date=game_date,
    )


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

    past_date = date(2020, 7, 4)
    print(f"--- Parsing Historical Example with no games ({past_date}) ---")
    historical_games_df = get_covers_games(past_date)
    print(historical_games_df)

    future_date = date(2025, 7, 4)
    print(f"\n--- Parsing Future Example with no games ({future_date}) ---")
    future_games_df = get_covers_games(future_date)
    print(future_games_df)

    past_date = date(2020, 11, 25)
    print(f"--- Parsing Historical Example With Canceled Games ({past_date}) ---")
    historical_games_df = get_covers_games(past_date)
    print(historical_games_df)

    future_date = date(2025, 4, 7)
    print(f"\n--- Parsing Future Example ({future_date}) ---")
    future_games_df = get_covers_games(future_date)
    print(future_games_df)
