-- Update na tabela [scania_parts_tab] - dados semanais
/*
UPDATE scania_parts_tab spt
SET market_price = mp.market_price,
    market_difference = spt.current_retail_price - mp.market_price
FROM (
    SELECT
        product_code,
        DATE_TRUNC('month', job_datetime)::DATE AS month_year,
        PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY product_price) AS market_price
    FROM competitors_products
    JOIN competitors USING (id_competitor)
    WHERE id_competitor_type IN (1, 3, 4)
        AND id_competitor <> 5
        AND DATE_TRUNC('month', job_datetime)::DATE = (SELECT DATE_TRUNC('month', MAX(job_datetime))::DATE FROM competitors_products)
    GROUP BY product_code, month_year
) AS mp
WHERE spt.part_number = mp.product_code
    AND spt.insertion_date = mp.month_year;
*/


-- Update na tabela [scania_parts_tab_history] - todos os meses

UPDATE scania_parts_tab_history spt
SET market_price = mp.market_price,
    market_difference = spt.current_retail_price - mp.market_price
FROM (
    WITH grouped AS (
        SELECT
            product_code,
            DATE_TRUNC('month', job_datetime)::DATE AS month_year,
            product_price,
            id_competitor
        FROM competitors_products
        GROUP BY product_code, month_year, product_price, id_competitor
        ORDER BY product_price
    )
    SELECT
        product_code,
        month_year,
        PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY product_price) AS market_price
    FROM grouped
    JOIN competitors USING (id_competitor)
    WHERE id_competitor_type NOT IN (2, 5)
        AND id_competitor <> 5
    GROUP BY product_code, month_year
) AS mp
WHERE spt.part_number = mp.product_code
    AND spt.insertion_date = mp.month_year;
