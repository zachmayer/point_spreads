"""Multi-date parser for Covers.com NCAA basketball data."""

from datetime import date, datetime, timedelta

import polars as pl
from tqdm import tqdm

from point_spreads.covers_parser import get_covers_games


def get_covers_games_for_dates(dates: list[date]) -> pl.DataFrame:
    """
    Fetch and combine game data for multiple specified dates.

    Dates during off-season months (May-October) are automatically excluded.

    Args:
        dates: List of dates to fetch game data for

    Returns:
        DataFrame containing combined game data from all dates
    """
    # Months when college basketball is not in season (May through October)
    off_season_months = {5, 6, 7, 8, 9, 10}
    filtered_dates = [d for d in dates if d.month not in off_season_months]

    # Get game data for each date
    dataframes: list[pl.DataFrame] = []

    # Create progress bar that will display the current date being processed
    progress_bar = tqdm(filtered_dates, desc="Fetching game data")
    for game_date in progress_bar:
        progress_bar.set_description(f"Processing {game_date.isoformat()}")
        games_df = get_covers_games(game_date)
        dataframes.append(games_df)

    # Concatenate the dataframes vertically - will return empty DataFrame if dataframes is empty
    return pl.concat(dataframes, how="vertical") if dataframes else pl.DataFrame()


def main() -> None:
    """
    Process spreads and totals data:
    1. Load the CSV file
    2. Filter for recent records
    3. Generate a list of unique dates to fetch
    4. Get game data for these dates
    5. Save results to CSV
    """
    # Load the CSV file
    csv_path = "data/spreads_and_totals.csv"

    df = pl.read_csv(csv_path)

    # Convert date columns to datetime
    df = df.with_columns([pl.col("game_date").str.to_datetime(), pl.col("updated_date").str.to_datetime()])

    # Filter for records where game_date is >= updated_date - 1
    filtered_df = df.filter(pl.col("game_date") >= pl.col("updated_date") - timedelta(days=1))

    # Start with existing dates from the DataFrame and convert to Python dates
    existing_dates = [d.date() for d in filtered_df.get_column("game_date").unique()]
    dates_set: set[date] = set(existing_dates)

    # Generate date range from min(today, last game date) to today + 8 days
    last_game_date = max(existing_dates)
    today = datetime.now().date()
    start_date = min(last_game_date, today)
    end_date = today + timedelta(days=8)

    # Add all these days to our set
    current_date = start_date
    while current_date <= end_date:
        dates_set.add(current_date)
        current_date += timedelta(days=1)

    # Convert back from a set to a sorted list
    dates_list = sorted(list(dates_set))

    # Get game data for these dates
    result_df = get_covers_games_for_dates(dates_list)

    # Save results to CSV
    output_path = "data/updated_games.csv"
    result_df.write_csv(output_path)
    print(f"Updated data saved to {output_path}")


if __name__ == "__main__":
    main()
