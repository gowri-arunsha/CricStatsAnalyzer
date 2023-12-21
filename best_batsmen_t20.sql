with Batting as (
  SELECT
  match_type as Match_type,
  match_type_number as match_id,
  batsman as Batsman,
  SUM(d.runs.batsman) AS batter_runs_per_match,
  COUNT(d.batsman) AS balls_faced_per_match

FROM
  `cric-stats-analyzer.cricsheet_data.cric_data`,
  UNNEST(deliveries) AS d

  where match_type = "T20"
  group by match_type,match_type_number,Batsman
)

  SELECT
    Batsman,
    SUM(b.batter_runs_per_match) AS Total_Runs,
    SUM(b.balls_faced_per_match) AS Total_balls,
    COUNT(DISTINCT b.match_id) AS Total_Innings,
    ROUND(AVG(b.batter_runs_per_match), 2) AS Average_Runs,
    AVG(b.batter_runs_per_match / b.balls_faced_per_match) * 100 AS Strike_Rate,
    MAX(b.batter_runs_per_match) AS Best_Score,
    COUNT(CASE WHEN b.batter_runs_per_match >= 100 THEN 1 END) AS Hundreds,
    COUNT(CASE WHEN b.batter_runs_per_match >= 50 AND b.batter_runs_per_match < 100 THEN 1 END) AS Fifties
  FROM Batting b
  GROUP BY Batsman
  ORDER BY Total_Runs DESC
