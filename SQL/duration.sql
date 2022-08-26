create table duration as

WITH entry_times AS (
  SELECT
  visitorid,
  min(event_at) AS event_at
  FROM events
  GROUP BY 1
),

-- Get the earliest endpoint event for units that have an endpoint.
endpoint_events AS (
  SELECT *
    FROM events
  WHERE event IN ('transaction')
),

first_endpoint_events AS (
  SELECT 
  *
    FROM (
      SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY visitorid ORDER BY event_at ASC) AS row_num
      FROM endpoint_events
    ) AS _
  WHERE row_num = 1
),

-- Define the censoring time to be the latest timestamp in the whole event log.
censoring AS (
  SELECT max(event_at) AS event_at FROM events
)

-- Put all the pieces together as a *duration table*.
SELECT 
entry_times.visitorid,
entry_times.event_at as entry_at,
endpt.event AS endpoint_type,
endpt.event_at AS endpoint_at,
COALESCE(endpt.event_at, censoring.event_at) as final_obs_at,
COALESCE(endpt.event_at, censoring.event_at) - entry_times.event_at as duration
FROM censoring, entry_times
LEFT JOIN first_endpoint_events AS endpt
USING(visitorid)