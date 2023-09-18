-- Insert na tabela [competitors_products] - todos os meses
INSERT INTO competitors_products (
	job_datetime,
	product_code,
	id_competitor,
	product_name,
	product_price,
	product_link,
	product_image_link,
	manufacture,
	variation
)
SELECT
    job_datetime,
    product_code,
    id_competitor,
    product_name,
    product_price,
    product_link,
    product_image_link,
    manufacture,
    variation
FROM (
    WITH distinct_products AS (
        SELECT
            MIN(id_competitors_products)                    AS id_filtered,
            DATE_TRUNC('day', job_datetime)::DATE           AS job_datetime,
            id_competitor,
            CASE 
                WHEN LENGTH(product_code) < 7 AND id_competitor_type <> 2 THEN LPAD(product_code, 7, '0')
                ELSE product_code END AS product_code,
            product_price
        FROM raw_data.raw_competitors_products
        JOIN public.competitors c USING (id_competitor)
        WHERE c.id_competitor_type IN (1, 3)
        GROUP BY 2, 3, 4, 5
    ), automakers_distinct_products AS (
        SELECT
            MIN(id_competitors_products)                    AS id_filtered,
            DATE_TRUNC('day', job_datetime)::DATE           AS job_datetime,
            id_competitor,
            product_code,
            product_price
        FROM raw_data.raw_competitors_products
        JOIN public.competitors c USING (id_competitor)
        WHERE c.id_competitor_type = 2
        GROUP BY 2, 3, 4, 5
    )
    SELECT
        id_competitors_products,
        DATE_TRUNC('day', rcp.job_datetime)::DATE           AS job_datetime,
        dp.product_code,
        rcp.id_competitor,
        rcp.product_name,
        rcp.product_price,
        rcp.product_link,
        rcp.product_image_link,
        rcp.product_info ->> 'marca'                        AS manufacture,
        pl.current_retail_price - rcp.product_price        AS variation
    FROM raw_data.raw_competitors_products rcp
    JOIN price_list pl
        ON part_number = product_code
        AND DATE_TRUNC('month', rcp.job_datetime)::DATE = pl.month_year
    JOIN distinct_products dp
        ON rcp.id_competitors_products = dp.id_filtered
    WHERE (LOWER(product_name) NOT LIKE '%kit%'
        OR LOWER(product_name) NOT LIKE '%jogo%')
        AND rcp.product_price <> 0 
        AND DATE_TRUNC('day', rcp.job_datetime)::DATE > (SELECT MAX(job_datetime) FROM competitors_products)
    UNION ALL 
    SELECT
        id_competitors_products,
        DATE_TRUNC('day', rcp.job_datetime)::DATE           AS job_datetime,
        CASE
            WHEN adp.product_code LIKE '0%' THEN LTRIM(adp.product_code, '0')
            ELSE adp.product_code END AS product_code,
        rcp.id_competitor,
        rcp.product_name,
        rcp.product_price,
        rcp.product_link,
        rcp.product_image_link,
        rcp.product_info ->> 'marca'                        AS manufacture,
        NULL        										AS variation
    FROM raw_data.raw_competitors_products rcp
    JOIN automakers_distinct_products adp
        ON rcp.id_competitors_products = adp.id_filtered
    WHERE (LOWER(product_name) NOT LIKE '%kit%'
        OR LOWER(product_name) NOT LIKE '%jogo%')
        AND rcp.product_price <> 0
        AND DATE_TRUNC('day', rcp.job_datetime)::DATE > (SELECT MAX(job_datetime) FROM competitors_products)
) AS competitor_treated_data;
