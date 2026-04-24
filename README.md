# Focus Elite Fitness Competition Database

This is a code repository for fetching, cleaning, and linking data for elite functional fitness competitions from a variety of sources. The code utilizes public APIs whenver possible to fetch json-formatted leaderboard data. If an API is not available, functions exist to parse HTML pages. In very few cases, leaderboards and workouts are manually extracted.

This codebase is integrated into the Google Cloud Platform. Raw data is stored in Google Cloud Storage in json format and is then transformed using Pydantic models into a relational database stored on BigQuery.

## Sources

### (CrossFit)[https://games.crossfit.com/]
Most of the events for the CrossFit Games season are available at games.crossfit.com/leaderboard. Many of the sanctionals and semifinal events are managed are organized by a third party and host their leaderboards elsewhere. Some events have been copied over to CrossFit's site, but most have not.

### (Strongest)[https://compete.strongest.com/]
This all-in-one event management and live-scoring platform hosts the leaderboards for The Rogue Invitational (since 2021), The Mayhem Classic (2020,2025,2026), and one Wodapalooza (2022).

### (Competition Corner)[https://competitioncorner.net/]
By far the most popular site for hosting competitions.

### (Hustle Up)[https://hustleup.app/]
App only. Only used for the 2025 Last Chance Qualfier.

## Code Structure

Event metadata, athlete profiles, workouts, and leaderboards are pulled through the (API request clients)[api.py]. The resulting JSON/HTML files are then stored on Google Cloud Storage with manager clients for each platform. The results are then parsed into (Pydantic models)[models.py] which extract and format the data so that it can be (uploaded)[upload.py] to into a relational database on Google Cloud Bigquery.

## Relational Database Structure

## CLI entry points

## Database API

## Snapshots