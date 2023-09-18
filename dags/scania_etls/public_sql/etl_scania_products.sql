-- Insert na tabela [scania_products] - atualizando com dados novos

INSERT INTO public.scania_products (
    part_number,
    part_status,
    prg,
    cc,
    dc,
    insertion_date,
    part_description
)
WITH newst_products AS (
    SELECT
        pl.part_number,
        MAX(pl.month_year)::DATE AS insertion_date
    FROM price_list pl
    GROUP BY pl.part_number
)
, catch_description AS (
    SELECT
        np.part_number,
        pl.part_status,
        pl.prg,
        pl.cc,
        pl.dc,
        np.insertion_date,
        pl.part_description
    FROM newst_products np
    JOIN price_list pl
        ON month_year = insertion_date
        AND np.part_number = pl.part_number
)
SELECT * FROM catch_description
ON CONFLICT (part_number)
DO UPDATE
SET part_status = EXCLUDED.part_status,
    prg = EXCLUDED.prg,
    cc = EXCLUDED.cc,
    dc = EXCLUDED.dc,
    insertion_date = EXCLUDED.insertion_date,
    part_description = EXCLUDED.part_description;
