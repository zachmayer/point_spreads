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
    for game_date in tqdm(filtered_dates, desc="Fetching game data"):
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

    # Start with existing dates from the DataFrame
    existing_dates = filtered_df.select("game_date").to_series().dt.date().unique().to_list()

    # Create a set for uniqueness
    dates_set: set[date] = set(existing_dates)

    # Add today and 7 days into the future
    today = datetime.now().date()
    for i in range(0, 8):
        future_date = today + timedelta(days=i)
        dates_set.add(future_date)

    # Sort the dates
    dates_list = sorted(list(dates_set))

    # Get game data for these dates
    result_df = get_covers_games_for_dates(dates_list)

    # Save results to CSV
    output_path = "data/updated_games.csv"
    result_df.write_csv(output_path)
    print(f"Updated data saved to {output_path}")


if __name__ == "__main__":
    main()
