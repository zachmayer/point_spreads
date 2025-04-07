"""Multi-date parser for Covers.com NCAA basketball data."""

from datetime import date, timedelta

import polars as pl
from tqdm import tqdm

from point_spreads.covers_parser import GAME_DATA_SCHEMA, get_covers_games


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
    return pl.concat(dataframes, how="vertical") if dataframes else pl.DataFrame(schema=GAME_DATA_SCHEMA)


def main() -> None:
    """
    Process spreads and totals data:
    1. Load the CSV file
    2. Filter for recent records
    3. Generate a list of unique dates to fetch
    4. Get game data for these dates
    5. Update existing data and append new data
    6. Save results to CSV
    """
    # Load the CSV file
    csv_path = "data/spreads_and_totals.csv"
    output_path = csv_path

    # Read existing data with the correct schema
    existing_df = pl.read_csv(csv_path, schema=GAME_DATA_SCHEMA)

    # Filter for records where game_date is >= updated_date - 1
    filtered_df = existing_df.filter(pl.col("game_date") >= pl.col("updated_date") - timedelta(days=1))

    # Extract game dates from the DataFrame
    existing_dates = filtered_df.select("game_date").unique().to_series().to_list()
    dates_set: set[date] = set(existing_dates)

    # Generate date range from min(today, last game date) to today + 8 days
    last_game_date = max(existing_dates)
    today = date.today()
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
    new_data_df = get_covers_games_for_dates(dates_list)

    # Drop rows with missing spreads
    new_data_df = new_data_df.filter(pl.col("spread").is_not_null())

    # Create composite keys for both dataframes
    key_expr = pl.concat_str(
        [pl.col("game_date").cast(pl.Utf8), pl.col("home_team"), pl.col("away_team")], separator="|"
    ).alias("composite_key")

    existing_df = existing_df.with_columns([key_expr])
    new_data_df = new_data_df.with_columns([key_expr])

    # Extract keys for comparison
    existing_keys = existing_df.select("composite_key").to_series().to_list()

    # Split new data into updates and inserts
    updates_df = new_data_df.filter(pl.col("composite_key").is_in(existing_keys))
    inserts_df = new_data_df.filter(~pl.col("composite_key").is_in(existing_keys))

    # Remove records to be updated from existing data
    update_keys = updates_df.select("composite_key").to_series().to_list()
    existing_df = existing_df.filter(~pl.col("composite_key").is_in(update_keys))

    # Drop the composite key column before combining
    existing_df = existing_df.drop("composite_key")
    updates_df = updates_df.drop("composite_key")
    inserts_df = inserts_df.drop("composite_key")

    # Combine existing data with updates and inserts
    all_dfs = [existing_df, updates_df, inserts_df]
    result_df = pl.concat(all_dfs, how="vertical")

    # Sort by game_date, home_team, away_team
    result_df = result_df.sort(["game_date", "home_team", "away_team"])

    # Save results to CSV
    result_df.write_csv(output_path)
    print(f"Updated data saved to {output_path}")
    print(f"Updated {len(updates_df)} records, added {len(inserts_df)} new records")


if __name__ == "__main__":
    main()
