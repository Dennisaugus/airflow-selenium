-- Insert na tabela [time_series] - dados mensais

-- INSERT INTO public.time_series (
-- 	part_number,
-- 	month_year,
-- 	sales_unit_price,
-- 	current_retail_price,
-- 	current_dealer_price,
-- 	current_factory_price,
-- 	base_cost_price,
-- 	demand,
-- 	revenue,
-- 	revenue_sm,
-- 	revenue_bu,
-- 	revenue_diff_last_month,
-- 	revenue_last_12_months,
-- 	revenue_diff_last_12_months,
-- 	percentage_in_revenue,
-- 	demand_last_12_months
-- )
-- WITH _price_list AS (
-- 	SELECT
-- 	    part_number,
-- 	    current_factory_price AS factory_price,
-- 	    current_dealer_price AS dealer_price,
-- 	    current_retail_price AS retail_price,
-- 	    base_cost_price AS base_cost,
-- 	    month_year
-- 	FROM public.price_list
-- )
-- , _sales_rede AS (
-- 	SELECT
-- 		part_number,
--         item_quantity,
--         sale_revenue,
--         month_year
--     FROM raw_data.raw_sales_rede
-- 	WHERE part_line IN (1, 45)
-- )
-- , price_list_and_sales_rede_concat AS (
-- 	SELECT
-- 		*,
-- 		CASE
-- 			WHEN ((sale_revenue / item_quantity) / retail_price) - 1 > 0.4
--                 THEN retail_price
-- 			ELSE (sale_revenue / item_quantity)
--                 END AS sale_unit_price
-- 	FROM _sales_rede sr
-- 	RIGHT JOIN _price_list pl USING (month_year, part_number)
-- )
-- , aggregation AS (
-- 	SELECT
-- 		part_number,
-- 		month_year,
-- 		PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sale_unit_price) AS sale_unit_price,
-- 		retail_price,
-- 		dealer_price,
-- 		factory_price,
-- 		base_cost,
-- 		COALESCE(SUM(item_quantity), 0) AS demand
-- 	FROM price_list_and_sales_rede_concat
-- 	GROUP BY 1, 2, 4, 5, 6, 7
-- )
-- , revenue_calc_month AS (
-- 	SELECT
-- 		*,
-- 		coalesce(sale_unit_price * demand, 0) AS revenue,
-- 		factory_price * demand AS revenue_sm,
-- 		dealer_price * demand AS revenue_bu
-- 	FROM aggregation agg
-- )
-- , revenue_calc_year_1 AS (
-- 	SELECT
-- 		*,
--         SUM(demand) OVER (PARTITION BY part_number ORDER BY month_year RANGE BETWEEN interval '11 months' PRECEDING AND CURRENT ROW) AS demand_last_12_months,
-- 		SUM(revenue) OVER (PARTITION BY part_number ORDER BY month_year RANGE BETWEEN interval '11 months' PRECEDING AND CURRENT ROW) AS year_revenue,
--         revenue - LAG(revenue, 1, NULL) OVER (PARTITION BY part_number ORDER BY month_year RANGE BETWEEN interval '1 month' PRECEDING AND CURRENT ROW) AS revenue_diff_last_month
--     FROM revenue_calc_month
-- )
-- , revenue_calc_year_2 AS (
-- 	SELECT
-- 		*,
-- 		year_revenue - LAG(year_revenue, 12, NULL) OVER (PARTITION BY part_number ORDER BY month_year RANGE BETWEEN interval '11 months' PRECEDING AND CURRENT ROW) AS year_revenue_diff
-- 	FROM revenue_calc_year_1
-- )
-- , percent_in_revenue_calc AS (
-- 	SELECT
-- 		*,
-- 		CASE
-- 			WHEN (SUM(revenue) OVER (PARTITION BY month_year)) = 0 THEN 0
-- 			ELSE revenue / (SUM(revenue) OVER (PARTITION BY month_year)) 
-- 		END AS percentage_in_revenue
-- 	FROM revenue_calc_year_2
-- )
-- SELECT
-- 	part_number,
-- 	month_year,
-- 	sale_unit_price,
-- 	retail_price,
-- 	dealer_price,
-- 	factory_price,
-- 	base_cost,
-- 	demand,
-- 	revenue,
-- 	revenue_sm,
-- 	revenue_bu,
-- 	revenue_diff_last_month,
-- 	year_revenue,
-- 	year_revenue_diff,
-- 	percentage_in_revenue,
-- 	demand_last_12_months
-- FROM percent_in_revenue_calc
-- WHERE retail_price IS NOT NULL
-- ON CONFLICT ON CONSTRAINT unique_part_number_month
-- DO UPDATE
-- SET demand = EXCLUDED.demand,
-- 	revenue = EXCLUDED.revenue,
-- 	revenue_bu = EXCLUDED.revenue_bu,
-- 	revenue_sm = EXCLUDED.revenue_sm,
-- 	revenue_diff_last_month = EXCLUDED.revenue_diff_last_month,
-- 	sales_unit_price = EXCLUDED.sales_unit_price,
-- 	revenue_last_12_months = EXCLUDED.revenue_last_12_months,
-- 	revenue_diff_last_12_months = EXCLUDED.revenue_diff_last_12_months,
-- 	percentage_in_revenue = EXCLUDED.percentage_in_revenue,
-- 	demand_last_12_months = EXCLUDED.demand_last_12_months;


-- Verificar a atualização dos dados de demanda, ... do mês anterior

INSERT INTO public.time_series (
	part_number,
	month_year,
	sales_unit_price,
	current_retail_price,
	current_dealer_price,
	current_factory_price,
	base_cost_price,
	demand,
	revenue,
	revenue_sm,
	revenue_bu,
	revenue_diff_last_month,
	revenue_last_12_months,
	revenue_diff_last_12_months,
	percentage_in_revenue,
	demand_last_12_months
)
WITH _price_list AS (
	SELECT
	    part_number,
	    current_factory_price AS factory_price,
	    current_dealer_price AS dealer_price,
	    current_retail_price AS retail_price,
	    base_cost_price AS base_cost,
	    month_year
	FROM public.price_list
)
, _sales_rede AS (
	SELECT
		part_number,
        item_quantity,
        sale_revenue,
        month_year
    FROM raw_data.raw_sales_rede
	WHERE part_line IN (1, 45)
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
, revenue_calc_year_1 AS (
	SELECT
		*,
        SUM(demand) OVER (PARTITION BY part_number ORDER BY month_year RANGE BETWEEN interval '11 months' PRECEDING AND CURRENT ROW) AS demand_last_12_months,
		SUM(revenue) OVER (PARTITION BY part_number ORDER BY month_year RANGE BETWEEN interval '11 months' PRECEDING AND CURRENT ROW) AS year_revenue,
        revenue - LAG(revenue, 1, NULL) OVER (PARTITION BY part_number ORDER BY month_year RANGE BETWEEN interval '1 month' PRECEDING AND CURRENT ROW) AS revenue_diff_last_month
    FROM revenue_calc_month
)
, revenue_calc_year_2 AS (
	SELECT
		*,
		year_revenue - LAG(year_revenue, 12, NULL) OVER (PARTITION BY part_number ORDER BY month_year RANGE BETWEEN interval '11 months' PRECEDING AND CURRENT ROW) AS year_revenue_diff
	FROM revenue_calc_year_1
)
, percent_in_revenue_calc AS (
	SELECT
		*,
		CASE
			WHEN (SUM(revenue) OVER (PARTITION BY month_year)) = 0 THEN 0
			ELSE revenue / (SUM(revenue) OVER (PARTITION BY month_year)) 
		END AS percentage_in_revenue
	FROM revenue_calc_year_2
)
SELECT
	part_number,
	month_year,
	sale_unit_price,
	retail_price,
	dealer_price,
	factory_price,
	base_cost,
	demand,
	revenue,
	revenue_sm,
	revenue_bu,
	revenue_diff_last_month,
	year_revenue,
	year_revenue_diff,
	percentage_in_revenue,
	demand_last_12_months
FROM percent_in_revenue_calc
WHERE retail_price IS NOT NULL
	AND month_year >= (SELECT MAX(month_year) - interval '1 month' FROM time_series)
ON CONFLICT ON CONSTRAINT unique_part_number_month
DO UPDATE
SET demand = EXCLUDED.demand,
	revenue = EXCLUDED.revenue,
	revenue_bu = EXCLUDED.revenue_bu,
	revenue_sm = EXCLUDED.revenue_sm,
	revenue_diff_last_month = EXCLUDED.revenue_diff_last_month,
	sales_unit_price = EXCLUDED.sales_unit_price,
	revenue_last_12_months = EXCLUDED.revenue_last_12_months,
	revenue_diff_last_12_months = EXCLUDED.revenue_diff_last_12_months,
	percentage_in_revenue = EXCLUDED.percentage_in_revenue,
	demand_last_12_months = EXCLUDED.demand_last_12_months;
