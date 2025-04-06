"""Parser for Covers.com NCAA basketball HTML data."""

from datetime import date

import pandas as pd
import requests
from lxml import html
from pydantic import BaseModel


# Pydantic model for game data
class GameData(BaseModel):
    game_date: date
    update_date: date
    home_team: str
    away_team: str
    spread: str
    total: str
    neutral_site: bool


def download_covers_html(game_date: date) -> str:
    """
    Download Covers.com HTML for a specific date.
    """
    date_str = game_date.strftime("%Y-%m-%d")
    url = f"https://www.covers.com/Sports/NCAAB/Matchups?selectedDate={date_str}"

    response = requests.get(url)
    response.raise_for_status()

    return response.text


def parse_future_games(html_content: str, game_date: date) -> pd.DataFrame:
    """
    Parse future games from Covers.com HTML content.
    """
    tree = html.fromstring(html_content)
    game_containers = tree.xpath('//article[contains(@class, "gamebox pregamebox")]')

    games: list[GameData] = []
    for container in game_containers:
        teams_text = container.xpath('string(.//p[@id="gamebox-header"]/strong[@class="text-uppercase"])').strip()
        away_team, home_team = [team.strip() for team in teams_text.split("@")]

        # Target the text node of the SECOND team-consensus span for the home spread
        spread_text = container.xpath(
            'string(.//span[contains(@class, "team-consensus")][2]/text()[normalize-space()])'
        ).strip()
        total_text = container.xpath('string(.//span[contains(@class, "team-overunder")])').strip()
        total_text = total_text.lower().replace("o/u ", "")
        neutral_site = "(N)" in container.xpath('string(.//p[@id="gamebox-header"])')

        game_data = GameData(
            game_date=game_date,
            update_date=date.today(),
            home_team=home_team,
            away_team=away_team,
            spread=spread_text,
            total=total_text,
            neutral_site=neutral_site,
        )
        games.append(game_data)

    return pd.DataFrame([game.model_dump() for game in games])


def parse_historical_games(html_content: str, game_date: date) -> pd.DataFrame:
    """
    Parse historical games from Covers.com HTML content
    """
    tree = html.fromstring(html_content)
    game_containers = tree.xpath('//article[contains(@class, "gamebox") and contains(@class, "postgamebox")]')

    # Use list of GameData model
    games: list[GameData] = []

    for container in game_containers:
        header_text = container.xpath(
            'string(.//p[contains(@class, "gamebox-header")]/strong[@class="text-uppercase"])'
        ).strip()
        away_team_raw, home_team_raw = header_text.split("@")
        away_team = away_team_raw.strip()
        home_team = home_team_raw.strip()

        spread_raw_text = container.xpath('string(.//p[contains(@class, "summary-box")]/strong[1])').strip()
        spread = spread_raw_text.upper()

        total_xpath = (
            ".//p[contains(@class, 'summary-box')]/strong[starts-with(normalize-space(text()), 'under ') "
            "or starts-with(normalize-space(text()), 'over ')]/text()"
        )
        total = container.xpath(total_xpath)[0].strip()
        # Remove the prefix ("under " or "over ")
        total = total.split(" ", 1)[-1]

        # Extract neutral site info from header paragraph text content
        header_full_text = container.xpath('string(.//p[contains(@class, "gamebox-header")])').lower()
        neutral_site = "(n)" in header_full_text or "neutral" in header_full_text or "tournament" in header_full_text

        # Create GameData instance
        game_data = GameData(
            game_date=game_date,
            update_date=date.today(),
            home_team=home_team,
            away_team=away_team,
            spread=spread,
            total=total,
            neutral_site=neutral_site,
        )
        games.append(game_data)

    # Ensure the DataFrame has the expected columns even if no games are found
    if not games:
        return pd.DataFrame(
            columns=["game_date", "update_date", "home_team", "away_team", "spread", "total", "neutral_site"]
        )

    # Create DataFrame from list of Pydantic models
    return pd.DataFrame([game.model_dump() for game in games])


def get_covers_games(game_date: date) -> pd.DataFrame:
    """
    Get games for a specific date from Covers.com.

    Args:
        game_date: Date to get games for

    Returns:
        DataFrame containing game information
    """
    # Download HTML content
    html_content = download_covers_html(game_date)

    # Choose the appropriate parser based on the date
    today = date.today()
    if game_date < today:
        return parse_historical_games(html_content, game_date)
    else:
        return parse_future_games(html_content, game_date)


# Example usage within this script
if __name__ == "__main__":
    past_date = date(2023, 3, 8)
    print(f"--- Parsing Historical Example ({past_date}) ---")
    html_past = download_covers_html(past_date)
    historical_games_df = parse_historical_games(html_past, past_date)
    print(historical_games_df)

    future_date = date(2025, 4, 7)
    print(f"\n--- Parsing Future Example ({future_date}) ---")
    html_future = download_covers_html(future_date)
    future_games_df = parse_future_games(html_future, future_date)
    print(future_games_df)
