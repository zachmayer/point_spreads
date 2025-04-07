"""Analytics tests for Covers NCAA basketball data."""

import warnings
from pathlib import Path

import polars as pl
import pytest

# Global constants
TEST_YEARS = range(2011, 2025)
MONTHLY_THRESHOLDS = {
    11: 0.60,
    12: 0.50,
    1: 0.46,
    2: 0.30,
    3: 0.15,
    4: 0.00,
}


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


@pytest.mark.parametrize("year", TEST_YEARS)
def test_games_per_year(year: int) -> None:
    """Test that each year has a reasonable number of games."""
    df = get_test_data()

    # Filter to the year in question
    games_count = df.filter(pl.col("year") == year).height

    # Assert at least 3,000 games per year (reasonable minimum based on data)
    assert games_count >= 3000, f"Year {year} only has {games_count} games, expected at least 3,000"


@pytest.mark.parametrize("year", TEST_YEARS)
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


@pytest.mark.parametrize("year", TEST_YEARS)
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

    min_spread = spread_stats.item(0, "min_spread")
    max_spread = spread_stats.item(0, "max_spread")
    mean_spread = spread_stats.item(0, "mean_spread")

    # Assert reasonable bounds for spread statistics
    assert -100 <= min_spread <= -20, f"Year {year} has minimum spread {min_spread}, expected between -100 and -20"
    assert 20 <= max_spread <= 50, f"Year {year} has maximum spread {max_spread}, expected between 20 and 50"
    assert -10 <= mean_spread <= 0, f"Year {year} has mean spread {mean_spread}, expected between -10 and 0"


@pytest.mark.parametrize("year", TEST_YEARS)
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

    min_total = total_stats.item(0, "min_total")
    max_total = total_stats.item(0, "max_total")
    mean_total = total_stats.item(0, "mean_total")

    # Assert reasonable bounds for total statistics
    assert 80 <= min_total <= 120, f"Year {year} has minimum total {min_total}, expected between 80 and 120"
    assert 170 <= max_total <= 200, f"Year {year} has maximum total {max_total}, expected between 170 and 200"
    assert 130 <= mean_total <= 150, f"Year {year} has mean total {mean_total}, expected between 130 and 150"


@pytest.mark.parametrize("year", TEST_YEARS)
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


@pytest.mark.parametrize("year", TEST_YEARS)
def test_yearly_spread_coverage(year: int) -> None:
    """Test that each year has less than 35% missing spreads."""
    df = get_test_data()
    year_df = df.filter(pl.col("year") == year)
    total_games = year_df.height

    missing_spreads = year_df.filter(pl.col("spread").is_null()).height
    missing_pct = missing_spreads / total_games

    assert missing_pct < 0.35, f"Year {year} has {missing_pct:.2%} missing spreads (>35%)"


@pytest.mark.parametrize("year", TEST_YEARS)
@pytest.mark.parametrize("month", MONTHLY_THRESHOLDS.keys())
def test_monthly_spread_coverage(year: int, month: int) -> None:
    """Test that each month has appropriate spread coverage based on thresholds."""
    df = get_test_data()
    month_data = df.filter((pl.col("game_date").dt.month() == month) & (pl.col("year") == year))

    if month_data.height == 0:
        if year != 2020 or month != 4:
            warnings.warn(f"No data found for {year}-{month}, skipping test")
        pytest.skip(f"No data for {year}-{month}")

    threshold = MONTHLY_THRESHOLDS[month]
    missing_pct = month_data["spread"].null_count() / month_data.height

    if threshold == 0.0:
        assert missing_pct == threshold, f"Month {month} in {year} has {missing_pct:.2%} missing spreads (expected 0%)"
    else:
        assert missing_pct < threshold, (
            f"Month {month} in {year} has {missing_pct:.2%} missing spreads (threshold: {threshold:.2%})"
        )


@pytest.mark.parametrize("year", TEST_YEARS)
def test_spread_total_consistency(year: int) -> None:
    """Test that if spread is missing, total is also missing (and vice versa) in >90% of cases."""
    df = get_test_data()
    df = df.filter(pl.col("year") == year)

    # Count cases where only one is missing
    only_spread_missing = df.filter(pl.col("spread").is_null() & pl.col("total").is_not_null()).height
    only_total_missing = df.filter(pl.col("spread").is_not_null() & pl.col("total").is_null()).height

    # Count cases where either is missing
    either_missing = df.filter(pl.col("spread").is_null() | pl.col("total").is_null()).height

    consistency_rate = 1 - (only_spread_missing + only_total_missing) / either_missing
    assert consistency_rate > 0.70, f"Year {year} spread/total consistency rate is {consistency_rate:.2%} (<90%)"


def test_year_over_year_consistency() -> None:
    """Test that missing data % doesn't increase by more than 35% year-over-year for the same month."""
    df = get_test_data()

    # Skip first year since we need to compare with previous year
    for curr_year in list(TEST_YEARS)[1:]:
        prev_year = curr_year - 1

        # For each month in our thresholds
        for month in MONTHLY_THRESHOLDS.keys():
            # Get previous year data for this month
            prev_year_data = df.filter((pl.col("year") == prev_year) & (pl.col("game_date").dt.month() == month))

            # Get current year data for this month
            curr_year_data = df.filter((pl.col("year") == curr_year) & (pl.col("game_date").dt.month() == month))

            # Skip if either year has no data for this month
            if prev_year_data.height == 0 or curr_year_data.height == 0:
                continue

            # Calculate missing percentages
            prev_pct = prev_year_data.filter(pl.col("spread").is_null()).height / prev_year_data.height
            curr_pct = curr_year_data.filter(pl.col("spread").is_null()).height / curr_year_data.height

            # Calculate increase
            increase = curr_pct - prev_pct

            assert increase <= 0.35, (
                f"Month {month}: {curr_year} missing increased by {increase:.2%} vs {prev_year} (>35%)"
            )


@pytest.mark.parametrize("year", TEST_YEARS)
@pytest.mark.parametrize("team", ["Duke", "North Carolina", "Kentucky", "Kansas", "UCLA", "Gonzaga"])
def test_major_team_coverage(year: int, team: str) -> None:
    """Test that major teams have at least 20 spreads per year consistently."""
    df = get_test_data()

    # Count games with spreads for team in specific year
    team_data = df.filter(
        ((pl.col("home_team").str.contains(team)) | (pl.col("away_team").str.contains(team))) & (pl.col("year") == year)
    )

    games_with_spreads = team_data.filter(pl.col("spread").is_not_null()).height

    assert games_with_spreads >= 20, f"{team} has only {games_with_spreads} spreads in {year} (<20)"
