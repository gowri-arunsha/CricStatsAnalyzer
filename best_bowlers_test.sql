with w1 as (
 SELECT
    DISTINCT match_type_number as match_id,
    d.bowler AS bowler,
    d.wickets.wicket.kind AS kind,
    d.wickets.wicket.player_out AS player_out
  FROM
    `cric-stats-analyzer.cricsheet_data.cric_data`,
    UNNEST(deliveries) AS d

  WHERE
    d.wickets.wicket.kind IS NOT NULL
    AND d.wickets.wicket.kind NOT IN ("stumped", "run out")
    and match_type = "Test"
),
 w2 as (
  SELECT
    bowler,
    match_id,
    COUNT(match_id) AS wickets_per_match
  FROM w1
  GROUP BY w1.bowler, match_id
  ORDER BY bowler
 ),

 j AS (
  SELECT
    bowler,
    match_type_number as match_id,
    d.over as overs,
    runs.total AS runs
  FROM
    `cric-stats-analyzer.cricsheet_data.cric_data`,
    UNNEST(deliveries) AS d
),

j2 AS (
  SELECT
    bowler,
    match_id,
    j.overs AS overs,
    SUM(runs) AS Runs_conceded_per_over
  FROM j
  GROUP BY match_id, bowler, overs
),

j3 AS (
  SELECT
    bowler,
    match_id,
    ((j2.Runs_conceded_per_over / 1) * 6) AS Economy_rate_per_over,
    overs
  FROM j2
),

j4 AS (
  SELECT
    bowler,
    match_id,
    AVG(Economy_rate_per_over) AS Economy_rate_per_match,
    COUNT(j3.overs) * 6 AS balls_per_match
  FROM j3
  GROUP BY bowler, match_id
  ORDER BY bowler
),
j5 as(
  SELECT
    w2.*,
    j4.Economy_rate_per_match,
    j4.balls_per_match,
    (j4.balls_per_match / w2.wickets_per_match) AS strike_rate_per_match
  FROM
    w2
  LEFT JOIN
    j4 ON w2.bowler = j4.bowler AND w2.match_id = j4.match_id
  GROUP BY
    w2.bowler, w2.match_id, w2.wickets_per_match, j4.Economy_rate_per_match, j4.balls_per_match

)

 SELECT
    bowler AS Name,
    SUM(wickets_per_match) AS Wickets,
    AVG(wickets_per_match) AS Average_Wickets,
    COUNT(match_id) AS Innings,
    AVG(Economy_rate_per_match) AS Economy_rate,
    AVG(strike_rate_per_match) AS Strike_rate,
    MAX(wickets_per_match) AS Best_bowling
  FROM
    j5
  GROUP BY bowler
