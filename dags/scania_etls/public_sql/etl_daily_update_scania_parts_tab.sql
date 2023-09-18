UPDATE scania_parts_tab spt
SET sales_unit_price = mp.sales_unit_price,
    demand = mp.demand,
    revenue = mp.revenue,
    pl3_price = mp.pl3_price,
    pl3_demand = mp.pl3_demand
FROM (
    SELECT
        insertion_date,
        part_number,
        sales_unit_price,
        demand,
        revenue,
        pl3_price,
        pl3_demand
    FROM scania_parts_tab_history
    WHERE insertion_date >= (SELECT MAX(insertion_date) - interval '1 month' FROM scania_parts_tab_history)
) AS mp
WHERE spt.part_number = mp.part_number
AND spt.insertion_date = mp.insertion_date;