UPDATE time_series ts
SET demand = EXCLUDED.demand,
	revenue = EXCLUDED.revenue,
	revenue_bu = EXCLUDED.revenue_bu,
	revenue_sm = EXCLUDED.revenue_sm,
	sales_unit_price = EXCLUDED.sales_unit_price
FROM (
    WITH _price_list AS (
        SELECT
            part_number,
            current_factory_price AS factory_price,
            current_dealer_price AS dealer_price,
            current_retail_price AS retail_price,
            base_cost_price AS base_cost,
            month_year
        FROM public.price_list
        WHERE month_year = (SELECT max(month_year) FROM raw_data.raw_sales_rede rsr)
    )
    , _sales_rede AS (
        SELECT
            part_number,
            item_quantity,
            sale_revenue,
            month_year
        FROM raw_data.raw_sales_rede
        WHERE part_line IN (1, 45)
        AND month_year = (SELECT max(month_year) FROM raw_data.raw_sales_rede)
    )
    , price_list_and_sales_rede_concat AS (
        SELECT
            *,
            CASE
                WHEN ((sale_revenue / item_quantity) / retail_price) - 1 > 0.4
                    THEN retail_price
                ELSE (sale_revenue / item_quantity)
                    END AS sale_unit_price
        FROM _sales_rede sr
        RIGHT JOIN _price_list pl USING (month_year, part_number)
    )
    , aggregation AS (
        SELECT
            part_number,
            month_year,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sale_unit_price) AS sale_unit_price,
            retail_price,
            dealer_price,
            factory_price,
            base_cost,
            COALESCE(SUM(item_quantity), 0) AS demand
        FROM price_list_and_sales_rede_concat
        GROUP BY 1, 2, 4, 5, 6, 7
    )
    , revenue_calc_month AS (
        SELECT
            *,
            COALESCE(sale_unit_price * demand, 0) AS revenue,
            factory_price * demand AS revenue_sm,
            dealer_price * demand AS revenue_bu
        FROM aggregation agg
    )
    SELECT rcm.part_number, rcm.month_year, rcm.demand, rcm.revenue, rcm.revenue_bu, rcm.revenue_sm
    FROM revenue_calc_month rcm
) AS daily_update_time_series
WHERE ts.part_number = daily_update_time_series.part_number
AND ts.insertion_date = daily_update_time_series.month_year;