INSERT INTO scania_parts_tab (
    part_number,
    prg,
    cc,
    dc,
    insertion_date,
    part_description,
    base_cost_price,
    current_factory_price,
    current_dealer_price,
    current_retail_price,
    current_sm_margin,
    current_bubr_margin,
    current_dealer_margin,
    percentage_in_revenue,
    market_price,
    market_difference,
    ipi_percentage,
    icms_percentage,
    pis_percentage,
    cofins_percentage,
    suggested_factory_price,
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
    current_dealer_net_price,
    suggested_dealer_net_price,
    demand,
    rrp_real,
    revenue,
    revenue_diff_last_month,
    sm_base_profit_real_calculated,
    bu_gross_profit_real_calculated,
    sales_unit_price,
    demand_last_12_months,
    current_bubr_margin_flag,
    current_sm_margin_flag,
    suggested_bubr_margin_flag,
    suggested_sm_margin_flag,
    premium_price,
    demand_last_month,
    revenue_last_month,
    revenue_last_12_months,
    maintenance_percentage,
    sm_percentage_in_profit,
    bu_percentage_in_profit,
    pl3_price,
    pl3_demand
)
SELECT
    part_number,
    prg,
    cc,
    dc,
    insertion_date,
    part_description,
    base_cost_price,
    current_factory_price,
    current_dealer_price,
    current_retail_price,
    current_sm_margin,
    current_bubr_margin,
    current_dealer_margin,
    percentage_in_revenue,
    market_price,
    market_difference,
    ipi_percentage,
    icms_percentage,
    pis_percentage,
    cofins_percentage,
    suggested_factory_price,
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
    current_dealer_net_price,
    suggested_dealer_net_price,
    demand,
    rrp_real,
    revenue,
    revenue_diff_last_month,
    sm_base_profit_real_calculated,
    bu_gross_profit_real_calculated,
    sales_unit_price,
    demand_last_12_months,
    current_bubr_margin_flag,
    current_sm_margin_flag,
    suggested_bubr_margin_flag,
    suggested_sm_margin_flag,
    premium_price,
    demand_last_month,
    revenue_last_month,
    revenue_last_12_months,
    maintenance_percentage,
    sm_percentage_in_profit,
    bu_percentage_in_profit,
    pl3_price,
    pl3_demand
FROM scania_parts_tab_history
WHERE insertion_date > (SELECT MAX(insertion_date) FROM scania_parts_tab);
