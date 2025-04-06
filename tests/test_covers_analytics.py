"""Analytics tests for Covers NCAA basketball data."""

from pathlib import Path

import polars as pl
import pytest


def get_test_data() -> pl.DataFrame:
    """Load the real historical spreads and totals data."""
    data_path = Path(__file__).parent.parent / "data" / "spreads_and_totals.csv"

    # Read CSV with explicit schema to ensure proper types
    df = pl.read_csv(
        data_path,
        has_header=True,
        new_columns=["game_date", "update_date", "home_team", "away_team", "spread", "total"],
        schema={
            "game_date": pl.Date,
            "update_date": pl.Date,
            "home_team": pl.Utf8,
            "away_team": pl.Utf8,
            "spread": pl.Float64,
            "total": pl.Float64,
        },
    )

    # Extract year as an integer column
    df_with_year = df.with_columns(pl.col("game_date").dt.year().alias("year"))

    print(df_with_year.head())
    return df_with_year


def test_dataset_metrics() -> None:
    """Test metrics about the NCAA basketball dataset."""
    df = get_test_data()

    # 1. Count rows by year
    rows_by_year = df.group_by("year").agg(pl.len().alias("count")).sort("year")

    print("\n--- Games per Year ---")
    print(rows_by_year)

    # 2. Count unique teams by year
    # Combine home and away teams into a single frame with year
    home_teams = df.select("year", pl.col("home_team").alias("team"))
    away_teams = df.select("year", pl.col("away_team").alias("team"))
    all_teams = pl.concat([home_teams, away_teams])

    unique_teams_by_year = all_teams.unique().group_by("year").agg(pl.len().alias("unique_teams")).sort("year")

    print("\n--- Unique Teams per Year ---")
    print(unique_teams_by_year)

    # 3. Count rows with unknown spreads and totals
    unknown_spreads = (
        df.filter(pl.col("spread").is_null()).group_by("year").agg(pl.len().alias("unknown_spreads")).sort("year")
    )

    unknown_totals = (
        df.filter(pl.col("total").is_null()).group_by("year").agg(pl.len().alias("unknown_totals")).sort("year")
    )

    print("\n--- Unknown Spreads per Year ---")
    print(unknown_spreads if not unknown_spreads.is_empty() else "No unknown spreads")
    print("\n--- Unknown Totals per Year ---")
    print(unknown_totals if not unknown_totals.is_empty() else "No unknown totals")

    # 4. Calculate min/mean/median/max of spread and total by year
    spread_stats = (
        df.group_by("year")
        .agg(
            pl.col("spread").min().alias("min"),
            pl.col("spread").mean().alias("mean"),
            pl.col("spread").median().alias("median"),
            pl.col("spread").max().alias("max"),
        )
        .sort("year")
    )

    total_stats = (
        df.group_by("year")
        .agg(
            pl.col("total").min().alias("min"),
            pl.col("total").mean().alias("mean"),
            pl.col("total").median().alias("median"),
            pl.col("total").max().alias("max"),
        )
        .sort("year")
    )

    print("\n--- Spread Statistics by Year ---")
    print(spread_stats)
    print("\n--- Total Statistics by Year ---")
    print(total_stats)

    # After seeing the actual values, we could add assertions like:
    # assert rows_by_year.filter(pl.col("year") == 2011).select("count")[0, 0] > 1000
    # assert unique_teams_by_year.filter(pl.col("year") == 2011).select("unique_teams")[0, 0] > 250
    # assert spread_stats.filter(pl.col("year") == 2011).select("min")[0, 0] < -20
    # etc.


@pytest.mark.parametrize("year", range(2011, 2025))
def test_games_per_year(year: int) -> None:
    """Test that each year has a reasonable number of games."""
    df = get_test_data()

    # Filter to the year in question
    games_count = df.filter(pl.col("year") == year).height

    # Assert at least 3,000 games per year (reasonable minimum based on data)
    assert games_count >= 3000, f"Year {year} only has {games_count} games, expected at least 3,000"


@pytest.mark.parametrize("year", range(2011, 2025))
def test_unique_teams_per_year(year: int) -> None:
    """Test that each year has a reasonable number of unique teams."""
    df = get_test_data()

    # Filter to the year in question
    year_df = df.filter(pl.col("year") == year)

    # Combine home and away teams and count unique ones
    home_teams = year_df.select(pl.col("home_team").alias("team"))
    away_teams = year_df.select(pl.col("away_team").alias("team"))
    all_teams = pl.concat([home_teams, away_teams])
    unique_team_count = all_teams.unique().height

    # Assert at least 300 unique teams per year (reasonable minimum based on data)
    assert unique_team_count >= 300, f"Year {year} only has {unique_team_count} unique teams, expected at least 300"


@pytest.mark.parametrize("year", range(2011, 2025))
def test_spread_stats_per_year(year: int) -> None:
    """Test that spread statistics are within reasonable bounds."""
    df = get_test_data()

    # Filter to the year in question and calculate spread stats
    year_df = df.filter(pl.col("year") == year)
    spread_stats = year_df.select(
        pl.col("spread").min().alias("min_spread"),
        pl.col("spread").max().alias("max_spread"),
        pl.col("spread").mean().alias("mean_spread"),
    )

    if spread_stats.height == 0:
        pytest.skip(f"No spread data available for year {year}")

    min_spread = spread_stats.item(0, "min_spread")
    max_spread = spread_stats.item(0, "max_spread")
    mean_spread = spread_stats.item(0, "mean_spread")

    # Assert reasonable bounds for spread statistics
    assert -100 <= min_spread <= -20, f"Year {year} has minimum spread {min_spread}, expected between -100 and -20"
    assert 20 <= max_spread <= 50, f"Year {year} has maximum spread {max_spread}, expected between 20 and 50"
    assert -10 <= mean_spread <= 0, f"Year {year} has mean spread {mean_spread}, expected between -10 and 0"


@pytest.mark.parametrize("year", range(2011, 2025))
def test_total_stats_per_year(year: int) -> None:
    """Test that total statistics are within reasonable bounds."""
    df = get_test_data()

    # Filter to the year in question and calculate total stats
    year_df = df.filter(pl.col("year") == year)
    total_stats = year_df.filter(pl.col("total").is_not_null()).select(
        pl.col("total").min().alias("min_total"),
        pl.col("total").max().alias("max_total"),
        pl.col("total").mean().alias("mean_total"),
    )

    if total_stats.height == 0:
        pytest.skip(f"No total data available for year {year}")

    min_total = total_stats.item(0, "min_total")
    max_total = total_stats.item(0, "max_total")
    mean_total = total_stats.item(0, "mean_total")

    # Assert reasonable bounds for total statistics
    assert 80 <= min_total <= 120, f"Year {year} has minimum total {min_total}, expected between 80 and 120"
    assert 170 <= max_total <= 200, f"Year {year} has maximum total {max_total}, expected between 170 and 200"
    assert 130 <= mean_total <= 150, f"Year {year} has mean total {mean_total}, expected between 130 and 150"


@pytest.mark.parametrize("year", range(2011, 2025))
def test_missing_data_per_year(year: int) -> None:
    """Test that missing data percentages are within reasonable bounds."""
    df = get_test_data()

    # Filter to the year in question
    year_df = df.filter(pl.col("year") == year)
    total_games = year_df.height

    # Count missing spreads and totals
    missing_spreads = year_df.filter(pl.col("spread").is_null()).height
    missing_totals = year_df.filter(pl.col("total").is_null()).height

    # Calculate percentages
    spread_missing_pct = (missing_spreads / total_games) * 100
    total_missing_pct = (missing_totals / total_games) * 100

    # Assert reasonable bounds for missing data
    assert spread_missing_pct < 40, f"Year {year} has {spread_missing_pct:.1f}% missing spreads, expected less than 40%"
    assert total_missing_pct < 40, f"Year {year} has {total_missing_pct:.1f}% missing totals, expected less than 40%"
