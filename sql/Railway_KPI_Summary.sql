-- Railway KPI Summary
-- Purpose: Executive-level railway operational KPIs

SELECT
    COUNT(*) AS total_train_records,
    COUNT(DISTINCT Source_Station_Name) AS unique_source_stations,
    COUNT(DISTINCT Destination_Station_Name) AS unique_destination_stations
FROM workspace.default.silver_railway;
