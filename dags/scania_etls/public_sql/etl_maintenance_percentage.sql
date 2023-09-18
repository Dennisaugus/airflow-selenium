-- Insert na tabela [maintenance_percentage] - dados mensais

INSERT INTO maintenance_percentage (
    part_number,
    month_year,
    quantity_maintenance,
    quantity_client,
    maintenance_percentage
)
WITH datafilter AS (
    SELECT DISTINCT month_year
    FROM raw_data.raw_sales_rede
    WHERE month_year >= (SELECT MAX(month_year) - interval '11 months' FROM raw_data.raw_sales_rede)
),
datafilterpricelist AS (
	SELECT MAX(month_year) AS month_year
	FROM price_list pl2
),
filterPreventiveCorrective AS (
    SELECT
        part_number,
        SUM(item_quantity) AS quantity_maintenance
    FROM raw_data.raw_sales_rede rsr
    WHERE part_line IN (1, 45)
        AND (sales_group != 'CL' OR sales_group IS NULL)
        AND month_year IN (
            SELECT *
            FROM datafilter
            JOIN (SELECT DISTINCT month_year FROM price_list pl) foo USING (month_year)
        )
    GROUP BY 1
),
filterCL AS (
    SELECT
        part_number,
        SUM(item_quantity) AS quantity_client
    FROM raw_data.raw_sales_rede rsr
    WHERE part_line IN (1, 45)
        AND sales_group = 'CL'
        AND month_year IN (
            SELECT *
            FROM datafilter
            JOIN (SELECT DISTINCT month_year FROM price_list pl) foo USING (month_year)
        )
    GROUP BY 1
),
filterSPT AS (
    SELECT DISTINCT part_number 
    FROM scania_parts_tab_history
)
SELECT
    part_number,
	dfpl.month_year,
    COALESCE(fpc.quantity_maintenance, 0) AS quantity_maintenance,
    COALESCE(fcl.quantity_client, 0) AS quantity_client,
    COALESCE(fpc.quantity_maintenance, 0) / (COALESCE(fpc.quantity_maintenance, 0) + COALESCE(fcl.quantity_client, 0))::float AS maintenance_percentage
FROM filterPreventiveCorrective fpc
FULL OUTER JOIN filterCL fcl USING (part_number)
JOIN filterSPT spt USING (part_number)
CROSS JOIN datafilterpricelist dfpl
