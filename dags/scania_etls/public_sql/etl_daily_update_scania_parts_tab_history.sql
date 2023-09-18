-- Update na tabela [scania_parts_tab_history] - dados semanais

-- UPDATE scania_parts_tab spt
-- SET sales_unit_price = mp.sales_unit_price,
--     demand = mp.demand,
--     revenue = mp.revenue,
--     revenue_diff_last_month = mp.revenue_diff_last_month
-- FROM (
--     SELECT
--         month_year,
--         part_number,
--         sales_unit_price,
--         demand,
--         revenue,
--         revenue_diff_last_month
--     FROM time_series
-- ) AS mp
-- WHERE spt.part_number = mp.part_number
--     AND spt.insertion_date = mp.month_year;


UPDATE scania_parts_tab_history spt
SET sales_unit_price = mp.sales_unit_price,
    demand = mp.demand,
    revenue = mp.revenue
FROM (
    SELECT
        month_year,
        part_number,
        sales_unit_price,
        demand,
        revenue
    FROM time_series
    WHERE month_year >= (SELECT MAX(month_year) - interval '1 month' FROM time_series)
) AS mp
WHERE spt.part_number = mp.part_number
AND spt.insertion_date = mp.month_year;
