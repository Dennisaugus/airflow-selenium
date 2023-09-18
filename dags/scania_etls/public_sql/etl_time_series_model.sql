INSERT INTO public.time_series_model (
part_number,
month_year,
sale_unit_price,
demand,
factory_price,
dealer_price,
shelf_price,
part_cost,
revenue,
revenue_sm,
revenue_bu,
dealer_margin,
revenue_diff_last_month,
year_revenue,
last_12months_qdemand,
last_12months_ndemand,
year_revenue_diff,
market_mean,
price_missmatch
)
WITH time_series_model AS (
SELECT "part_number",
	"month_year",
	"sale_unit_price",
	"demand",
	"factory_price",
	"dealer_price",
	"shelf_price",
	"part_cost",
	"revenue",
	"revenue_sm",
	"revenue_bu",
	"dealer_margin",
	"revenue_diff_last_month",
	"year_revenue",
	"last_12months_qdemand",
	"last_12months_ndemand",
	"year_revenue_diff",
	"market_mean",
	"sale_unit_price" - "market_mean" AS "price_missmatch"
FROM (
	SELECT "LHS"."part_number" AS "part_number",
		"LHS"."month_year" AS "month_year",
		"sale_unit_price",
		"demand",
		"factory_price",
		"dealer_price",
		"shelf_price",
		"part_cost",
		"revenue",
		"revenue_sm",
		"revenue_bu",
		"dealer_margin",
		"revenue_diff_last_month",
		"year_revenue",
		"last_12months_qdemand",
		"last_12months_ndemand",
		"year_revenue_diff",
		"market_mean"
	FROM (
		SELECT "part_number",
		"month_year",
		"sale_unit_price",
		"demand",
		"factory_price",
		"dealer_price",
		"shelf_price",
		"part_cost",
		"revenue",
		"revenue_sm", 
		"revenue_bu", 
		"dealer_margin", 
		"revenue_diff_last_month", 
		"year_revenue", 
		"last_12months_qdemand", 
		"last_12months_ndemand", 
		"year_revenue" - LAG("year_revenue", 12, NULL) OVER (PARTITION BY part_number ORDER BY month_year RANGE BETWEEN interval '11 months' PRECEDING AND CURRENT ROW) AS year_revenue_diff
		FROM (
			SELECT "part_number", 
			"month_year", 
			"sale_unit_price", 
			"demand", 
			"factory_price", 
			"dealer_price", 
			"shelf_price", 
			"part_cost", 
			"revenue", 
			"revenue_sm", 
			"revenue_bu",
			"dealer_margin", 
			SUM(revenue) OVER (PARTITION BY part_number ORDER BY month_year RANGE BETWEEN interval '11 months'PRECEDING AND CURRENT ROW) AS "year_revenue",
			"revenue" - LAG("revenue", 1, NULL) OVER (PARTITION BY "part_number" ORDER BY "month_year") AS "revenue_diff_last_month",
			SUM(demand) OVER (PARTITION BY part_number ORDER BY month_year ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS "last_12months_qdemand",
			COUNT(*) FILTER (WHERE demand != 0) OVER (PARTITION BY part_number ORDER BY month_year ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS "last_12months_ndemand"
			FROM (
				SELECT "part_number", 
				"month_year", 
				"sale_unit_price", 
				"demand", 
				"factory_price", 
				"dealer_price", 
				"shelf_price", 
				"part_cost", 
				"sale_unit_price" * "demand" AS "revenue", 
				"factory_price" * "demand" AS "revenue_sm", 
				"dealer_price" * "demand" AS "revenue_bu",
				("sale_unit_price" - "part_cost") * "demand" AS "dealer_margin"
				FROM (
					SELECT "part_number", 
					"month_year", 
					CASE 
						WHEN ((("sale_unit_price") IS NULL)) THEN ("shelf_price")
						WHEN NOT((("sale_unit_price") IS NULL)) THEN ("sale_unit_price")
					END AS "sale_unit_price",
					CASE 
						WHEN ((("demand") IS NULL)) THEN (0.0) 
						WHEN NOT((("demand") IS NULL)) THEN ("demand") 
					END AS "demand", 
					"factory_price", 
					"dealer_price", 
					"shelf_price", 
					"part_cost"
					FROM (
						SELECT "RHS"."part_number" AS "part_number", 
						"RHS"."month_year" AS "month_year", 
						"sale_unit_price", 
						"demand", 
						"factory_price", 
						"dealer_price", 
						"shelf_price", 
						"part_cost"
						FROM (
							SELECT "part_number", 
							"month_year", 
							PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "sale_unit_price") AS "sale_unit_price", 
							SUM("item_quantity") AS "demand"
							FROM (
								SELECT "id_dealer", 
									"part_number", 
									"item_quantity", 
									"sale_revenue", 
									"month_year", 
									"sale_date", 
									"part_line", 
									"os_type", 
									"sales_group", 
									"fprice_real", 
									"dealer_price", 
									"shelf_price", 
									"part_cost", 
									CASE 
										WHEN ("diff_shelf_sale_unit_price" > 0.4) THEN ("shelf_price")
										WHEN NOT("diff_shelf_sale_unit_price" > 0.4) THEN ("sale_unit_price") 
									END AS "sale_unit_price", 
									"diff_shelf_sale_unit_price"
								FROM (
									SELECT "id_dealer", 
									"part_number", 
									"item_quantity", 
									"sale_revenue", 
									"month_year", 
									"sale_date", 
									"part_line", 
									"os_type", 
									"sales_group", 
									"fprice_real", 
									"dealer_price", 
									"shelf_price", 
									"part_cost", 
									"sale_unit_price", 
									("sale_unit_price" / "shelf_price") - 1.0 AS "diff_shelf_sale_unit_price"
									FROM (
										SELECT "id_dealer", 
										"part_number", 
										"item_quantity", 
										"sale_revenue", 
										"month_year", 
										"sale_date", 
										"part_line", 
										"os_type", 
										"sales_group", 
										"fprice_real", 
										"dealer_price", 
										"shelf_price", 
										"part_cost", 
										"sale_revenue" / "item_quantity" AS "sale_unit_price"
										FROM (
											SELECT "id_dealer",
											"LHS"."part_number" AS "part_number", 
											"item_quantity", 
											"sale_revenue", 
											"LHS"."month_year" AS "month_year", 
											"sale_date", 
											"part_line", 
											"os_type", 
											"sales_group", 
											"fprice_real", 
											"dealer_price", 
											"shelf_price", 
											"part_cost"
											FROM (
												SELECT *
												FROM "raw_data"."raw_sales_rede"
												WHERE (("part_line" IN (1.0, 45.0))
												AND ("month_year" <= (select max(month_year) - interval '1 month' from raw_data.raw_price_list_bu))
												AND ("month_year" >= '2018-01-01'))) "LHS"
												LEFT JOIN (
													SELECT "part_number",
													"fprice_real", 
													"dealer_price", 
													"shelf_price", 
													"part_cost", 
													"month_year"
													FROM "raw_data"."raw_price_list_bu") "RHS"
												ON ("LHS"."part_number" = "RHS"."part_number" AND "LHS"."month_year" = "RHS"."month_year")
										) "q01"
									) "q02"
								) "q03"
							) "q04"
							GROUP BY "part_number", "month_year") "LHS"
							RIGHT JOIN (
								SELECT "part_number",
								"fprice_real" AS "factory_price",
								"dealer_price",
								"shelf_price",
								"part_cost",
								"month_year"
								FROM "raw_data"."raw_price_list_bu"
								WHERE (("month_year" <= (select max(month_year) - interval '1 month' from raw_data.raw_price_list_bu)) 
								AND ("month_year" >= '2018-01-01'))) "RHS"
							ON ("LHS"."part_number" = "RHS"."part_number" AND "LHS"."month_year" = "RHS"."month_year")
					) "q01"
				) "q02"
			) "q03"
		) "q04"
	) "LHS"
	LEFT JOIN (
		SELECT "month_year",
		"part_number",
		PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "product_price") AS "market_mean"
		FROM (
			SELECT "job_datetime", 
			"product_code", 
			"product_price", 
			DATE_TRUNC('month', date("job_datetime")) AS "month_year", 
			"product_code" AS "part_number"
			FROM (
				SELECT "id_competitors_products",
				"job_datetime", 
				"product_code", 
				"LHS"."id_competitor" AS "id_competitor", 
				"product_name", 
				"product_price", 
				"product_link", 
				"product_image_link", 
				"manufacture", 
				"variation", 
				"competitor_name", 
				"id_competitor_type", 
				"competitor_website", 
				"automaker"
				FROM (
					SELECT *
					FROM "competitors_products"
				) "LHS"
			LEFT JOIN "competitors" AS "RHS"
			ON ("LHS"."id_competitor" = "RHS"."id_competitor")
			) "q01"
			WHERE ("id_competitor_type" IN (1.0, 3.0))
		) "q02"
		GROUP BY "month_year", "part_number") "RHS"
	ON ("LHS"."part_number" = "RHS"."part_number" AND "LHS"."month_year" = "RHS"."month_year")
) "q03"
)
SELECT *
FROM time_series_model tsm
WHERE month_year >= (SELECT MAX(month_year) - interval '1 month' FROM time_series_model)
ON CONFLICT ON CONSTRAINT unique_pn_months
DO UPDATE
SET demand = EXCLUDED.demand,
	revenue = EXCLUDED.revenue,
	revenue_bu = EXCLUDED.revenue_bu,
	revenue_sm = EXCLUDED.revenue_sm,
    dealer_margin = EXCLUDED.dealer_margin,
	revenue_diff_last_month = EXCLUDED.revenue_diff_last_month,
	sale_unit_price = EXCLUDED.sale_unit_price,
	year_revenue = EXCLUDED.year_revenue,
	year_revenue_diff = EXCLUDED.year_revenue_diff,
	market_mean = EXCLUDED.market_mean,
	price_missmatch = EXCLUDED.price_missmatch