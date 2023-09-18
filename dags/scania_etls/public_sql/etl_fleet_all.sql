-- Insert na tabela [fleet] - todos os dados

INSERT INTO public.fleet(
    model,
    year_model,
    quantity
)
SELECT
    model,
    year_model,
    COUNT(1) AS raw_quantity
FROM raw_data.raw_fleet
GROUP BY model, year_model;
