-- Train Activity by Day
-- Purpose: Analyze railway activity across the week

SELECT
    day_name,
    total_trains,
    ROUND(percentage_of_total, 2) AS percentage_of_total
FROM workspace.default.gold_day_summary
ORDER BY
    CASE day_name
        WHEN 'Monday' THEN 1
        WHEN 'Tuesday' THEN 2
        WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4
        WHEN 'Friday' THEN 5
        WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
    END;
