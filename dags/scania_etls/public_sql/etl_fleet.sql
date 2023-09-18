-- Insert na tabela [fleet]

INSERT INTO public.fleet(
    model,
    year_model,
    quantity
)
WITH raw_agg AS (
    SELECT
        model,
        year_model,
        COUNT(1) AS raw_quantity
    FROM raw_data.raw_fleet
    WHERE month_year = (SELECT MAX(month_year) FROM raw_data.raw_fleet)
    GROUP BY model, year_model
),
fleet_now AS (
    SELECT
        model,
        year_model,
        quantity
    FROM fleet
)
SELECT
    fn.model,
    fn.year_model,
    (fn.quantity + ra.raw_quantity) AS quantity
FROM fleet_now fn
RIGHT JOIN raw_agg ra
    ON fn.model = ra.model
    AND fn.year_model = ra.year_model
WHERE fn.model = ra.model
    AND fn.year_model = ra.year_model;
