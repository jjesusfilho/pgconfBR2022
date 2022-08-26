
CREATE TABLE survival AS

WITH num_subjects AS (
  SELECT COUNT(1) AS num_subjects FROM durations
),

duration_rounded AS (
  SELECT 
  visitorid,
  endpoint_type,
  ceil(extract(epoch FROM duration)/(24 * 60 * 60)) AS duration_days
  FROM durations
),

daily_tally AS (
  SELECT
  duration_days,
  COUNT(1) AS num_obs,
  SUM(
    CASE
    WHEN endpoint_type IS NOT NULL THEN 1
    ELSE 0
    END
  ) AS events
  FROM duration_rounded
  GROUP BY 1
),

cumulative_tally AS (
  SELECT 
  duration_days,
  num_obs,
  events,
  num_subjects - COALESCE(SUM(num_obs) OVER (
    ORDER BY duration_days ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0
  ) AS at_risk
  FROM daily_tally, num_subjects
)

SELECT
duration_days,
at_risk,
num_obs,
events,
at_risk - events - COALESCE(lead(at_risk, 1) OVER (ORDER BY duration_days ASC), 0) AS censored,

EXP(SUM(LN(1 - events / at_risk)) OVER (
  ORDER BY duration_days ASC ROWS BETWEEN UNBOUNDED PRECEDING AND current ROW)
) AS survival_proba,

100 * (1 - EXP(SUM(LN(1 - events / at_risk)) OVER (
  ORDER BY duration_days ASC ROWS BETWEEN UNBOUNDED PRECEDING AND current ROW))
) AS conversion_pct,

SUM(events / at_risk) OVER (
  ORDER BY duration_days ASC ROWS BETWEEN UNBOUNDED PRECEDING AND current ROW
) AS cumulative_hazard

FROM cumulative_tally
WHERE events > 0