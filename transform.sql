-- PA-1 Election Data Transforms
-- Run these against the BigQuery dataset: project-7634b355-01fb-4460-96c.pa1_election
-- ─────────────────────────────────────────────────────────────────────────────

-- 1. Democratic vote share by precinct across 2022 and 2024
--    Shows how competitive each precinct is and how Dem performance shifted.
SELECT
  election_year,
  county,
  precinct,
  SUM(CASE WHEN party = 'DEM' THEN votes ELSE 0 END) AS dem_votes,
  SUM(CASE WHEN party = 'REP' THEN votes ELSE 0 END) AS rep_votes,
  SUM(votes) AS total_votes,
  ROUND(
    SAFE_DIVIDE(
      SUM(CASE WHEN party = 'DEM' THEN votes ELSE 0 END),
      SUM(votes)
    ) * 100, 2
  ) AS dem_vote_share_pct
FROM `project-7634b355-01fb-4460-96c.pa1_election.precinct_results`
GROUP BY election_year, county, precinct
ORDER BY election_year, county, precinct;


-- 2. Total votes by county and year
--    Tracks turnout trends in the two counties that make up PA-1.
SELECT
  election_year,
  county,
  SUM(CASE WHEN party = 'DEM' THEN votes ELSE 0 END) AS dem_votes,
  SUM(CASE WHEN party = 'REP' THEN votes ELSE 0 END) AS rep_votes,
  SUM(votes) AS total_votes,
  ROUND(
    SAFE_DIVIDE(
      SUM(CASE WHEN party = 'DEM' THEN votes ELSE 0 END),
      SUM(votes)
    ) * 100, 2
  ) AS dem_vote_share_pct
FROM `project-7634b355-01fb-4460-96c.pa1_election.precinct_results`
GROUP BY election_year, county
ORDER BY county, election_year;


-- 3. Precincts where Democratic vote share improved from 2022 to 2024
--    These are opportunity targets: precincts trending toward Democrats
--    where additional organizing or GOTV could flip the seat in 2026.
WITH precinct_shares AS (
  SELECT
    county,
    precinct,
    election_year,
    ROUND(
      SAFE_DIVIDE(
        SUM(CASE WHEN party = 'DEM' THEN votes ELSE 0 END),
        SUM(votes)
      ) * 100, 2
    ) AS dem_vote_share_pct
  FROM `project-7634b355-01fb-4460-96c.pa1_election.precinct_results`
  GROUP BY county, precinct, election_year
),
pivoted AS (
  SELECT
    county,
    precinct,
    MAX(CASE WHEN election_year = 2022 THEN dem_vote_share_pct END) AS share_2022,
    MAX(CASE WHEN election_year = 2024 THEN dem_vote_share_pct END) AS share_2024
  FROM precinct_shares
  GROUP BY county, precinct
)
SELECT
  county,
  precinct,
  share_2022,
  share_2024,
  ROUND(share_2024 - share_2022, 2) AS improvement_pct
FROM pivoted
WHERE share_2022 IS NOT NULL
  AND share_2024 IS NOT NULL
  AND share_2024 > share_2022
ORDER BY improvement_pct DESC;
