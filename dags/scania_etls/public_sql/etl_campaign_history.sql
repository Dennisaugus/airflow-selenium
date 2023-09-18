-- Insert na tabela [campaign_history]

INSERT INTO public.campaign_history (
    campaign_id,
    part_number,
    prg,
    cc,
    dc,
    month_year,
    factory_price,
    dealer_price,
    retail_price,
    sales_price,
    market_price,
    demand,
    profit_sm,
    profit_bu,
    profit_dealer,
    revenue_sm,
    revenue_bu,
    revenue_dealer
)
WITH part_numbers AS (
    SELECT campaign_id, part_number
    FROM campaign_part_numbers
)
SELECT
    pn.campaign_id,
    spth.part_number,
    spth.prg,
    spth.cc,
    spth.dc,
    spth.insertion_date AS month_year,
    spth.current_factory_price AS factory_price,
    spth.current_dealer_price AS dealer_price,
    spth.current_retail_price AS retail_price,
    spth.sales_unit_price AS sales_price,
    spth.market_price,
    spth.demand,
    (spth.sm_base_profit_real_calculated * spth.demand) AS profit_sm,
    (spth.bu_gross_profit_real_calculated * spth.demand) AS profit_bu,
    ((spth.current_retail_price - spth.current_dealer_price) * spth.demand) AS profit_dealer,
    CASE
        WHEN spth.insertion_date = (SELECT MAX(insertion_date) FROM scania_parts_tab_history)
            THEN NULL
        ELSE ts.revenue_sm
    END,
    CASE
        WHEN spth.insertion_date = (SELECT MAX(insertion_date) FROM scania_parts_tab_history)
            THEN NULL
        ELSE ts.revenue_bu
    END,
    CASE
        WHEN spth.insertion_date = (SELECT MAX(insertion_date) FROM scania_parts_tab_history)
            THEN NULL
        ELSE ts.revenue
    END AS revenue_dealer
FROM part_numbers pn
JOIN campaigns c USING (campaign_id)
JOIN scania_parts_tab_history spth USING (part_number)
JOIN time_series ts
    ON ts.month_year = spth.insertion_date
    AND ts.part_number = spth.part_number
WHERE spth.insertion_date <= (SELECT MAX(insertion_date) FROM scania_parts_tab_history)
    AND spth.insertion_date <= c.campaign_end_date
	AND spth.insertion_date > (SELECT MAX(month_year) - interval '1 month' FROM campaign_history)
ON CONFLICT ON CONSTRAINT unique_campaign_id_part_number_month_year
DO UPDATE
SET sales_price = EXCLUDED.sales_price,
	market_price = EXCLUDED.market_price,
	demand = EXCLUDED.demand,
	profit_sm = EXCLUDED.profit_sm,
	profit_bu = EXCLUDED.profit_bu,
	profit_dealer = EXCLUDED.profit_dealer,
	revenue_sm = EXCLUDED.revenue_sm,
	revenue_bu = EXCLUDED.revenue_bu,
	revenue_dealer = EXCLUDED.revenue_dealer;
