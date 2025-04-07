"""Parser for Covers.com NCAA basketball HTML data."""

import datetime
from datetime import date as date_type
from typing import Any, TypeVar

import polars as pl
import requests
from diskcache import FanoutCache  # type: ignore
from lxml import html
from pydantic import BaseModel, Field
from pydantic_ai import Agent, ModelHTTPError
from pydantic_ai.settings import ModelSettings
from tenacity import retry, stop_after_attempt, wait_random_exponential

from point_spreads.settings import settings

# Create a cache with unlimited size in the project root directory
cache_dir = settings.cache_dir
cache_dir.mkdir(exist_ok=True)
cache = FanoutCache(directory=str(cache_dir))


# Define the schema for game data - this can be imported by other modules
GAME_DATA_SCHEMA = {
    "game_date": pl.Date,
    "updated_date": pl.Date,
    "home_team": pl.Utf8,
    "away_team": pl.Utf8,
    "home_team_spread": pl.Utf8,
    "away_team_spread": pl.Utf8,
    "total": pl.Utf8,
    "neutral_location": pl.Boolean,
    "tournament": pl.Utf8,
}


class CalendarDate(BaseModel):
    """Structured representation of a date from the calendar navigation."""

    date: date_type = Field(description="The date extracted from the calendar navigation")


class GameData(BaseModel):
    """Fields to extract from game HTML - doesn't include dates which we already know."""

    home_team: str = Field(description="Home team name (full name from header)")
    away_team: str = Field(description="Away team name (full name from header)")
    home_team_spread: str = Field(description="Spread for the home team (e.g., '-3.5' if home team is favorite)")
    away_team_spread: str = Field(description="Spread for the away team (e.g., '+3.5' if away team is underdog)")
    total: str = Field(description="Over/under total points (just the number without 'o/u' prefix)")
    neutral_location: bool = Field(
        False, description="True if game is at a neutral location (indicated by '(N)' in header)"
    )
    tournament: str = Field("", description="Tournament name if available (e.g., 'March Madness', 'Conf Tourn.')")


class GameDataWithDates(GameData):
    """Complete game data including dates."""

    game_date: date_type = Field(description="Date of the game")
    updated_date: date_type = Field(description="Date when the data was last updated")


# Define shared model settings for deterministic behavior
MODEL_SETTINGS: ModelSettings = {
    "temperature": 0.0,
    "seed": 42,
    "top_p": 1.0,
}


# Define the calendar date parser agent
calendar_parser_agent = Agent(
    "google-gla:gemini-2.0-flash",
    result_type=CalendarDate,
    model_settings=MODEL_SETTINGS,
    system_prompt="""
    You are a specialized parser for Covers.com calendar dates.

    You are given HTML content containing a calendar navigation bar like this:
    ```
    <div id="covers-CoversScoreboard-league-next-and-prev">
        <a href="/sports/ncaab/matchups?selectedDate=2023-03-30">
            <svg>...</svg>
        </a>
        <a href="/sports/ncaab/matchups?selectedDate=2023-03-28" class="navigation-anchor">
            <div class="day">Tue</div>
            <div class="date">Mar 28</div>
        </a>
        <a href="/sports/ncaab/matchups?selectedDate=2023-03-30" class="navigation-anchor">
            <div class="day">Thu</div>
            <div class="date">Mar 30</div>
        </a>
        <a class="navigation-anchor active isDailySport" aria-current="page">
            <div class="day">Sat</div>
            <div class="date">Apr 01</div>
        </a>
        <a href="/sports/ncaab/matchups?selectedDate=2023-04-03" class="navigation-anchor">
            <div class="day">Mon</div>
            <div class="date">Apr 03</div>
        </a>
        <a href="/sports/ncaab/matchups?selectedDate=2023-04-03">
            <svg>...</svg>
        </a>
    </div>
    ```

    Your task is to:

    1. Find the date element marked as "active" or "current" (has class "active" and/or aria-current="page")
    2. Extract the month and day from this element
    3. Determine the correct year by looking at the nearby date links in the href attributes
    4. Handle year transitions (Dec to Jan or Jan to Dec) correctly

    The key insight is that the active element shows the actual date the page is displaying,
    but it doesn't include the year. You must infer the year from surrounding date links.

    Return a single date object in the format YYYY-MM-DD.
    """,
)


# Define the game box parser agent
gamebox_parser_agent = Agent(
    "google-gla:gemini-2.0-flash",
    result_type=GameData,
    model_settings=MODEL_SETTINGS,
    system_prompt="""
    You are a specialized parser for NCAA basketball game information from Covers.com HTML.

    Extract the following information from the provided HTML game box:
    1. Home team name (use the full name from the header, e.g., "Houston" not "HOU")
    2. Away team name (use the full name from the header, e.g., "Florida" not "FLA")
    3. Home team spread - in the team's perspective
       (e.g., "-3.5" if home team is favorite, "+3.5" if home team is underdog)
    4. Away team spread - in the team's perspective
       (e.g., "+3.5" if away team is underdog, "-3.5" if away team is favorite)
    5. Total over/under points (as a string, just the number without "o/u" prefix)
    6. Neutral location (boolean, true if "(N)" appears in the header)
    7. Tournament name (if available, use empty string if none found)

    For spreads, careful analysis is required:

    - If explicit spreads for both teams are visible, use those values directly.
    - In historical (postgame) situations, carefully determine which team the spread applies to:
      - Look for phrases like "covered the spread of", "failed to cover", "underdog" or "favorite"
      - Determine which team (home or away) is being referenced
      - Assign the appropriate spread to that team
      - Calculate the opposite spread for the other team (if home is -3.5, away must be +3.5)
    - In future game situations, the spread is typically shown from the home team's perspective
      - If the home team has a negative spread (e.g., -3.5), they are favored
      - If the home team has a positive spread (e.g., +3.5), they are the underdog
      - The away team's spread will be the opposite (home -3.5 means away +3.5)

    Examples for determining spreads:
    1. "San Diego St covered the spread of -8.0" and San Diego St is the home team:
       - home_team_spread = "-8.0", away_team_spread = "+8.0"
    2. "Michigan State covered the spread of +5.5" and Michigan State is the away team:
       - away_team_spread = "+5.5", home_team_spread = "-5.5"
    3. "Favorite UConn failed to cover -12.5" and UConn is the home team:
       - home_team_spread = "-12.5", away_team_spread = "+12.5"
    4. Future game shows Houston -6.5 as the home team:
       - home_team_spread = "-6.5", away_team_spread = "+6.5"

    IMPORTANT: Always ensure that the two spreads are opposites (if one is +3.5, the other must be -3.5).

    Return values as strings for spreads and totals, maintaining the original format with signs.
    If a value is unavailable, use a reasonable default or empty string.
    """,
)


T = TypeVar("T")


@retry(
    wait=wait_random_exponential(multiplier=5, max=180, exp_base=3),
    stop=stop_after_attempt(10),
    retry=lambda retry_state: (
        retry_state.outcome is not None
        and isinstance(retry_state.outcome.exception(), ModelHTTPError)
        and hasattr(retry_state.outcome.exception(), "status_code")
        and retry_state.outcome.exception().status_code == 429  # type: ignore
    ),
)
def _run_agent_with_retry(agent: Agent[Any, T], content: str) -> T:
    """Run the specified Pydantic AI Agent with retry logic for 429 errors."""
    result = agent.run_sync(content)
    return result.data


# Define an empty DataFrame schema that matches the GameData model
def get_empty_dataframe() -> pl.DataFrame:
    """Create an empty DataFrame with the correct schema for game data."""
    return pl.DataFrame(schema=GAME_DATA_SCHEMA)


@retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(5))
@cache.memoize(typed=True, expire=settings.cache_ttl, tag="html")
def download_covers_html(game_date: date_type) -> str:
    """
    Download Covers.com HTML for a specific date.
    """
    date_str = game_date.strftime("%Y-%m-%d")
    url = f"https://www.covers.com/Sports/NCAAB/Matchups?selectedDate={date_str}"

    response = requests.get(url)
    response.raise_for_status()

    return response.text


def extract_date_from_calendar(html_content: str) -> date_type:
    """
    Extract the displayed date from the calendar navigation using the AI agent.

    Args:
        html_content: HTML content as a string

    Returns:
        date object representing the date displayed on the page
    """
    tree = html.fromstring(html_content)

    # Find the calendar navigation container
    calendar = tree.xpath('//div[@id="covers-CoversScoreboard-league-next-and-prev"]')
    if not calendar:
        # If calendar not found, return a date that will not match expected_date
        return datetime.date(1970, 1, 1)

    # Extract just the calendar HTML to reduce token usage
    calendar_html = html.tostring(calendar[0], encoding="unicode")

    # Use the agent with retry to parse the date from the calendar HTML
    result = _run_agent_with_retry(calendar_parser_agent, calendar_html)
    return result.date


@cache.memoize(typed=True, expire=settings.cache_ttl, tag="parsed-html")
def _parse_games(
    html_content: str,
    expected_date: date_type,
) -> pl.DataFrame:
    """
    Parse games from Covers.com HTML, handling both pre-game and post-game containers.

    Args:
        html_content: HTML content as a string.
        expected_date: Date of the games.

    Returns:
        DataFrame containing game information.
    """
    # Extract the date from the calendar navigation
    page_date = extract_date_from_calendar(html_content)

    print(f"Page date: {page_date}")
    print(f"Expected date: {expected_date}")

    # Validate the date matches what we expected
    if page_date != expected_date:
        print(f"Warning: Page date {page_date} does not match expected date {expected_date}")
        return get_empty_dataframe()

    # Parse the HTML content
    tree = html.fromstring(html_content)

    # Unified container XPath that gets all game boxes
    containers = tree.xpath('//article[contains(@class, "gamebox")]')
    games: list[GameDataWithDates] = []

    # Get today's date for the updated_date field
    today = datetime.date.today()

    for container in containers:
        # Convert the container to HTML string for Pydantic AI parsing
        container_html = html.tostring(container, encoding="unicode")

        # Use the agent with retry to parse the game box
        extracted_data = _run_agent_with_retry(gamebox_parser_agent, container_html)

        # Create a complete GameDataWithDates object by adding the dates
        game_data = GameDataWithDates(**extracted_data.model_dump(), game_date=expected_date, updated_date=today)

        print(game_data)
        games.append(game_data)

    if not games:
        return get_empty_dataframe()

    return pl.DataFrame([game.model_dump() for game in games])


def get_covers_games(game_date: date_type) -> pl.DataFrame:
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
    past_date = datetime.date(2023, 3, 8)
    print(f"--- Parsing Historical Example ({past_date}) ---")
    historical_games_df = get_covers_games(past_date)
    print(historical_games_df)

    past_date = datetime.date(2023, 3, 18)
    print(f"--- Parsing Historical Example ({past_date}) ---")
    historical_games_df = get_covers_games(past_date)
    print(historical_games_df)

    past_date = datetime.date(2024, 11, 13)
    print(f"--- Parsing Historical Example ({past_date}) ---")
    historical_games_df = get_covers_games(past_date)
    print(historical_games_df)

    past_date = datetime.date(2020, 7, 4)
    print(f"--- Parsing Historical Example with no games ({past_date}) ---")
    historical_games_df = get_covers_games(past_date)
    print(historical_games_df)

    future_date = datetime.date(2025, 7, 4)
    print(f"\n--- Parsing Future Example with no games ({future_date}) ---")
    future_games_df = get_covers_games(future_date)
    print(future_games_df)

    past_date = datetime.date(2020, 11, 25)
    print(f"--- Parsing Historical Example With Canceled Games ({past_date}) ---")
    historical_games_df = get_covers_games(past_date)
    print(historical_games_df)

    future_date = datetime.date(2025, 4, 7)
    print(f"\n--- Parsing Future Example ({future_date}) ---")
    future_games_df = get_covers_games(future_date)
    print(future_games_df)
