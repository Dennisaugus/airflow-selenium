UPDATE campaign_history ch
SET sales_price = mp.sales_price,
	market_price = mp.market_price,
	demand = mp.demand,
	profit_sm = mp.profit_sm,
	profit_bu = mp.profit_bu,
	profit_dealer = mp.profit_dealer,
	revenue_sm = mp.revenue_sm,
	revenue_bu = mp.revenue_bu,
	revenue_dealer = mp.revenue_dealer
FROM (
    WITH part_numbers AS (
        SELECT campaign_id, part_number
        FROM campaign_part_numbers
    )
    SELECT
        pn.campaign_id,
        spth.part_number,
        spth.insertion_date AS month_year,
        spth.sales_unit_price AS sales_price,
        spth.demand,
        (spth.sm_base_profit_real_calculated * spth.demand) AS profit_sm,
        (spth.bu_gross_profit_real_calculated * spth.demand) AS profit_bu,
        ((spth.current_retail_price - spth.current_dealer_price) * spth.demand) AS profit_dealer,
        CASE
            WHEN spth.insertion_date = (SELECT MAX(insertion_date) FROM scania_parts_tab_history)
                THEN NULL
            ELSE ts.revenue_sm
        END AS revenue_sm,
        CASE
            WHEN spth.insertion_date = (SELECT MAX(insertion_date) FROM scania_parts_tab_history)
                THEN NULL
            ELSE ts.revenue_bu
        END AND AS revenue_bu,
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
) AS mp
WHERE ch.part_number = mp.part_number
AND ch.month_year = mp.month_year;
