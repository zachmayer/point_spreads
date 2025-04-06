"""Parser for Covers.com NCAA basketball HTML data."""

from datetime import date, datetime

import pandas as pd
import requests
from lxml import html
from pydantic import BaseModel


class GameData(BaseModel):
    game_date: date
    update_date: date
    home_team: str
    away_team: str
    spread: str
    total: str


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
) -> pd.DataFrame:
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
        return pd.DataFrame(columns=list(GameData.model_fields.keys()))

    # Extract game data
    game_containers = tree.xpath(container_xpath)
    games: list[GameData] = []
    for container in game_containers:
        teams_text = container.xpath(f"string({teams_xpath})").strip()
        spread_text = container.xpath(f"string({spread_xpath})").strip()
        total_text = container.xpath(f"string({total_xpath})").strip()

        away_team_raw, home_team_raw = teams_text.split("@")  # Assumes '@' separator; will raise error if not
        away_team = away_team_raw.strip()
        home_team = home_team_raw.strip()
        spread = spread_text.upper()
        total_cleaned = total_text.lower().replace("o/u ", "").replace("under ", "").replace("over ", "")

        game_data = GameData(
            game_date=expected_date,
            update_date=date.today(),
            home_team=home_team,
            away_team=away_team,
            spread=spread,
            total=total_cleaned,
        )
        games.append(game_data)

    if not games:
        return pd.DataFrame(columns=list(GameData.model_fields.keys()))

    return pd.DataFrame([game.model_dump() for game in games])


def get_covers_games(game_date: date) -> pd.DataFrame:
    """
    Get games for a specific date from Covers.com.
    """
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
        container_xpath = '//article[contains(@class, "gamebox") and contains(@class, "postgamebox")]'
        teams_xpath = './/p[contains(@class, "gamebox-header")]/strong[@class="text-uppercase"]'
        spread_xpath = './/p[contains(@class, "summary-box")]/strong[1]'
        total_xpath = (
            ".//p[contains(@class, 'summary-box')]/strong[starts-with(normalize-space(text()), 'under ') "
            "or starts-with(normalize-space(text()), 'over ')]"
        )
    else:
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
