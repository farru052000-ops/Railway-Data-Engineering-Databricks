-- Top Source Stations
-- Purpose: Identify high-volume railway origin stations

SELECT
    Source_Station_Name,
    total_trains
FROM workspace.default.gold_station_summary
ORDER BY total_trains DESC
LIMIT 10;
