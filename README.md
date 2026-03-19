# PA-1 Election Pipeline

An ETL pipeline that ingests Pennsylvania congressional district 1 precinct-level election results into BigQuery for political analysis.

## Why PA-1?

Pennsylvania's 1st congressional district is one of the most competitive swing districts in the country. Covering the eastern half of Bucks County and a slice of Montgomery County, it has been a Democratic target since the 2018 blue wave reshaped Pennsylvania's map.

Republican incumbent Brian Fitzpatrick has held the seat since 2017, winning by narrow margins in both 2022 and 2024 against Democrat Ashley Ehasz. With Fitzpatrick consistently outrunning the top of the Republican ticket in a district where Biden and Harris performed well, PA-1 is a prime 2026 pickup opportunity. The key question is which specific precincts are trending Democratic and how much room remains to grow turnout.

## Data Source

[MIT Election Data + Science Lab (MEDSL)](https://github.com/MEDSL) — Official precinct-level general election results:

| Year | Source |
|------|--------|
| 2022 | `MEDSL/2022-elections-official` — `individual_states/2022-pa-local-precinct-general.zip` |
| 2024 | `MEDSL/2024-elections-official` — `individual_states/pa24.zip` |

> **Note:** 2025 data does not exist for this pipeline — U.S. House elections only occur in even-numbered years. The next race is November 2026.

## Setup

```bash
# Requires Python 3.11+ (arm64 on Apple Silicon)
arch -arm64 python3 -m venv venv
source venv/bin/activate
pip install pandas google-cloud-bigquery pyarrow requests
```

Authentication uses Application Default Credentials:

```bash
gcloud auth application-default login
```

## Running the pipeline

```bash
arch -arm64 venv/bin/python ingest.py
```

This will:
1. Download 2022 and 2024 PA precinct results from MEDSL GitHub (~2 MB)
2. Filter to PA-1 US House races (Bucks + Montgomery counties)
3. Normalize party labels to `DEM` / `REP` / `OTHER`
4. Load 1,444 rows into BigQuery: `pa1_election.precinct_results`

The table is overwritten on each run (`WRITE_TRUNCATE`).

## BigQuery schema

Table: `project-7634b355-01fb-4460-96c.pa1_election.precinct_results`

| Column | Type | Description |
|--------|------|-------------|
| `election_year` | INTEGER | 2022 or 2024 |
| `county` | STRING | Bucks or Montgomery |
| `precinct` | STRING | Precinct name/code |
| `office` | STRING | Always "US House" |
| `district` | STRING | Always "1" (PA-1) |
| `candidate` | STRING | Candidate full name |
| `party` | STRING | DEM, REP, or OTHER |
| `votes` | INTEGER | Votes cast |

## SQL analysis (`transform.sql`)

**Query 1 — Dem vote share by precinct:** Shows how competitive each precinct is and how performance changed between 2022 and 2024. Identifies base precincts vs. swing precincts.

**Query 2 — Total votes by county and year:** Tracks turnout trends across Bucks and Montgomery counties.

**Query 3 — Opportunity targets (precincts trending Dem):** Finds every precinct where the Democratic vote share *improved* from 2022 to 2024, ranked by the size of improvement. These are the precincts where 2026 organizing dollars will have the highest ROI.
