INSERT INTO public.competitors_products(
    job_datetime,
    product_code,
    id_competitor,
    product_name,
    product_price,
    product_link,
    variation
)
WITH FilterScaniaPartsTab AS (
    SELECT DISTINCT dpn.part_number, dpn.part_description, dpn.current_retail_price, dpn.insertion_date
    FROM scania_parts_tab_history dpn
),
PL3 AS (
    SELECT
        part_number,
        month_year,
        sale_date,
        sale_revenue/item_quantity AS sales_unit_price
    FROM raw_data.raw_sales_rede
    WHERE part_line IN (3, 10)
)
SELECT
    pl.sale_date AS job_datetime,
    pl.part_number AS product_code,
    19 AS id_competitor,
    fspt.part_description AS product_name,
    pl.sales_unit_price AS product_price,
    '' AS product_link,
    fspt.current_retail_price - pl.sales_unit_price AS variation
FROM PL3 pl
JOIN FilterScaniaPartsTab fspt
    ON pl.part_number = fspt.part_number
    AND pl.month_year = fspt.insertion_date
WHERE pl.month_year > (SELECT MAX(job_datetime) FROM competitors_products WHERE id_competitor = 19);


-- Update na tabela [competitors_products] - todos os dados
-- Atualização do pl3_price e pl3_demand na scania_parts_tab_history

UPDATE scania_parts_tab_history spt
SET pl3_price = pl3.pl3_price,
    pl3_demand = pl3.pl3_demand
FROM (
    SELECT
        part_number,
        month_year,
        PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY sale_revenue/item_quantity) AS pl3_price,
        COALESCE(SUM(item_quantity), 0) AS pl3_demand
	FROM raw_data.raw_sales_rede
	WHERE part_line IN (3, 10)
    AND month_year >= (SELECT max(month_year) - interval '1 month' FROM raw_data.raw_sales_rede)
	GROUP BY 1, 2
) AS pl3
WHERE spt.part_number = pl3.part_number
AND spt.insertion_date = pl3.month_year;