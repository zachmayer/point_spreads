# Point Spreads

Historical data for men's college basketball point spreads and totals.

## Example data

|game_date  |updated_date |home_team          |away_team     | spread| total|
|:----------|:------------|:------------------|:-------------|------:|-----:|
|2010-11-25 |2015-12-31   |Alaska - Anchorage |Weber St      |    9.5| 134.0|
|2010-11-25 |2015-12-31   |Arizona St         |Houston Bap   |  -20.5| 140.0|
|2010-11-25 |2015-12-31   |CS Northridge      |Virginia Tech |   17.5| 139.0|
|2010-11-25 |2015-12-31   |California         |Temple        |    5.5| 135.0|
|2010-11-25 |2015-12-31   |Manhattan          |Wisconsin     |   19.0| 132.5|

* **game_date**: date the game was/will be played
* **updated_date**: when the data for this game was last updated
* **home_team**: Home team
* **away_team**: Away team
* **spread**: the pre-game point spread for the game
* **total**: the pre-game point total for the game

## Data history

Professors Michael J. Lopez and Gregory Matthews won the [2014 March Mania Kaggle competition](https://www.kaggle.com/c/march-machine-learning-mania-2014/leaderboard) using a database of point spreads from 2010-2014. [They wrote a paper about it too!](https://arxiv.org/abs/1412.0248)

In 2016, [Bryan Cole updated this database with data from 2015](https://www.kaggle.com/competitions/march-machine-learning-mania-2016/discussion/19090).

I did some manual cleanup of this database, and have been updating it myself ever since. The main data source is [covers.com](https://www.covers.com/sports/ncaab/matchups).

This dataset took a ton of work to manually curate, and every year I scramble to update the data and fix data quality problems in time to submit for the Kaggle competition. I'm open-sourcing this repo so others who are interested in this data can help me maintain it, so we can all do less work to get our annual Kaggle submissions done.

I also plan to set up GitHub Action to automatically update this data during the next season, which will hopefully also provide an early warning about any future data quality problems.

## Data sources

* [covers.com](https://www.covers.com/sports/ncaab/matchups)
* [NCAA Basketball Scores And Odds Archives](https://www.sportsbookreviewsonline.com/scoresoddsarchives/ncaabasketball/ncaabasketballoddsarchives.htm)
* [The Prediction Tracker](https://www.thepredictiontracker.com/basketball.php)

* Covers' live site goes back to 2016-2017
* NCAA Basketball Scores And Odds Archives goes back to 2007-2008
* The Prediction Tracker goes back to 2003-2004

The database currently goes back to 2010-2011. I have not yet tried to clean up and append the 2003-2010 data, but this work could be done in the future. Check out [data/other_sources/](data/other_sources/) to see the 2003-2010 data from prediction tracker and 2007-2011 for the scores and odds archive. Once these files are integrated with the main DB, I will delete them.

### Non spread/totals

* [Sports Odds History](https://www.sportsoddshistory.com/college-basketball-odds/) has some good data on the history of tournament futures (the odds of each team winning the championship).
* [Ken Pomeroy's](https://kenpom.com/) Efficiency ratings are great.

## Project Structure

* **Code**: [`src/point_spreads/`](src/point_spreads/)
* **Data**: [`data/`](data/)

## Development

```bash
make all
```
