UPDATE scania_parts_tab_history spt
SET revenue_diff_last_month = mp.revenue_diff_last_month
FROM (
    SELECT
        month_year,
        part_number,
        revenue_diff_last_month
    FROM time_series
    WHERE month_year = (SELECT MAX(month_year) - interval '1 month' FROM time_series)
) AS mp
WHERE spt.part_number = mp.part_number
AND spt.insertion_date = mp.month_year;