-- Insert na tabela [scania_parts_tab] - mês mais recente

INSERT INTO scania_parts_tab_history (
    part_number,
    prg,
    cc,
    dc,
    insertion_date,
    part_description,
    base_cost_price,
    current_factory_price,
    current_dealer_net_price,
    current_dealer_price,
    current_retail_price,
    current_sm_margin,
    current_bubr_margin,
    current_dealer_margin,
    percentage_in_revenue,
    ipi_percentage,
    icms_percentage,
    pis_percentage,
    cofins_percentage,
    suggested_factory_price,
    suggested_dealer_net_price,
    suggested_dealer_price,
    suggested_retail_price,
    suggested_sm_margin,
    suggested_bubr_margin,
    suggested_dealer_margin,
    breakeven_point,
    demand_next_12_months_best_price,
    demand_next_12_months_retail_price,
    revenue_next_12_months_best_price,
    revenue_next_12_months_retail_price,
    demand,
    rrp_real,
    revenue,
    revenue_diff_last_month,
    sm_base_profit_real_calculated,
    bu_gross_profit_real_calculated,
    demand_last_12_months,
    demand_last_month,
    revenue_last_month,
    revenue_last_12_months,
    maintenance_percentage,
    sm_percentage_in_profit,
    bu_percentage_in_profit
)
WITH two_month_scania_parts_tab AS (
    WITH percentage_in_profit_calc AS (
        SELECT
            pl.part_number,
            pl.month_year,
            CASE
                WHEN (SUM(pl.sm_base_profit_real_calculated * ts.demand) OVER (PARTITION BY ts.month_year)) = 0 THEN 0
                ELSE (pl.sm_base_profit_real_calculated * ts.demand) / (SUM(pl.sm_base_profit_real_calculated * ts.demand) OVER (PARTITION BY ts.month_year))
            END AS sm_percentage_in_profit,
            CASE
                WHEN (SUM(pl.bu_gross_profit_real_calculated * ts.demand) OVER (PARTITION BY ts.month_year)) = 0 THEN 0
                ELSE (pl.bu_gross_profit_real_calculated * ts.demand) / (SUM(pl.bu_gross_profit_real_calculated * ts.demand) OVER (PARTITION BY ts.month_year))
            END AS bu_percentage_in_profit
        FROM price_list pl
        LEFT JOIN time_series ts
            ON pl.part_number = ts.part_number
            AND pl.month_year = ts.month_year
    )
    SELECT 
        pl.part_number,
        pl.prg,
        pl.cc,
        pl.dc,
        pl.month_year AS insertion_date,
        pl.part_description,
        pl.base_cost_price,
        pl.current_factory_price,
        pl.current_dealer_net_price,
        pl.current_dealer_price,
        pl.current_retail_price,
        pl.current_sm_margin,
        pl.current_bubr_margin,
        pl.current_dealer_margin,
        COALESCE(LAG(ts.percentage_in_revenue, 1, NULL) OVER (PARTITION BY ts.part_number ORDER BY ts.month_year RANGE BETWEEN interval '1 month' PRECEDING AND CURRENT ROW), 0) AS percentage_in_revenue,
        (pl.ipi_percentage / 100) AS ipi_percentage,
        (pl.icms_percentage / 100) AS icms_percentage,
        (pl.pis_percentage / 100) AS pis_percentage,
        (pl.cofins_percentage / 100) AS cofins_percentage,
        mapc.suggested_factory_price,
        mapc.suggested_dealer_net_price,
        mapc.suggested_dealer_price,
        mapc.suggested_retail_price,
        mapc.suggested_sm_margin,
        mapc.suggested_bubr_margin,
        mapc.suggested_dealer_margin,
        mapc.breakeven_point,
        mapc.demand_next_12_months_best_price,
        mapc.demand_next_12_months_retail_price,
        mapc.revenue_next_12_months_best_price,
        mapc.revenue_next_12_months_retail_price,
        ts.demand,
        (pl.rrp_real * 2.9242) AS rrp_real, -- esse 2 representa o valor do dólar no mês
        ts.revenue AS revenue,
        ts.revenue_diff_last_month,
        pl.sm_base_profit_real_calculated,
        pl.bu_gross_profit_real_calculated,
        ts.demand_last_12_months,
        LAG(ts.demand, 1, NULL) OVER (PARTITION BY ts.part_number ORDER BY ts.month_year RANGE BETWEEN interval '1 month' PRECEDING AND CURRENT ROW) AS demand_last_month,
        LAG(ts.revenue, 1, NULL) OVER (PARTITION BY ts.part_number ORDER BY ts.month_year RANGE BETWEEN interval '1 month' PRECEDING AND CURRENT ROW) AS revenue_last_month,
        LAG(ts.revenue_last_12_months, 1, NULL) OVER (PARTITION BY ts.part_number ORDER BY ts.month_year RANGE BETWEEN interval '1 month' PRECEDING AND CURRENT ROW) AS revenue_last_12_months,
        mp.maintenance_percentage,
        LAG(pipc.sm_percentage_in_profit, 1, NULL) OVER (PARTITION BY pipc.part_number ORDER BY pipc.month_year RANGE BETWEEN interval '1 month' PRECEDING AND CURRENT ROW) AS sm_percentage_in_profit,
        LAG(pipc.bu_percentage_in_profit, 1, NULL) OVER (PARTITION BY pipc.part_number ORDER BY pipc.month_year RANGE BETWEEN interval '1 month' PRECEDING AND CURRENT ROW) AS bu_percentage_in_profit
    FROM price_list pl
    LEFT JOIN time_series ts
        ON pl.part_number = ts.part_number
        AND pl.month_year = ts.month_year
    LEFT JOIN margin_and_price_calculations mapc
        ON pl.part_number = mapc.part_number
        AND pl.month_year = mapc.insertion_date
    LEFT JOIN maintenance_percentage mp
        ON pl.part_number = mp.part_number
        AND pl.month_year = mp.month_year
    LEFT JOIN percentage_in_profit_calc pipc
        ON pl.part_number = pipc.part_number
        AND pl.month_year = pipc.month_year
)
SELECT *
FROM two_month_scania_parts_tab tmspt
WHERE tmspt.insertion_date = (SELECT MAX(insertion_date) + interval '1 month' FROM scania_parts_tab_history);

UPDATE scania_parts_tab_history
SET demand =  NULL,
    revenue = NULL
WHERE insertion_date = (SELECT MAX(insertion_date) FROM scania_parts_tab_history)
