-- Insert na tabela [scania_prg_tab] - dados mensais
/*
INSERT INTO scania_prg_tab (
    insertion_date,
    prg,
    cc,
    dc,
    demand_next_12_months_retail_price,
    demand_next_12_months_best_price,
    profit_sm_next_12_months_retail_price,
    profit_sm_next_12_months_best_price,
    profit_bu_next_12_months_retail_price,
    profit_bu_next_12_months_best_price,
    harmonization,
    best_price_index,
    best_price_index_category
)
SELECT
    MAX(insertion_date) AS insertion_date,
    prg,
	cc,
	dc,
	SUM( COALESCE(demand_next_12_months_retail_price, 0) ) AS demand_next_12_months_retail_price,
	SUM( COALESCE(demand_next_12_months_best_price, 0) ) AS demand_next_12_months_best_price,
	SUM( COALESCE(sm_base_profit_real_calculated * demand_next_12_months_retail_price, 0) ) AS profit_sm_next_12_months_retail_price,
	SUM( COALESCE((suggested_factory_price - base_cost_price) * demand_next_12_months_best_price, 0) ) AS profit_sm_next_12_months_best_price,
	SUM( COALESCE(bu_gross_profit_real_calculated * demand_next_12_months_retail_price, 0) ) AS profit_bu_next_12_months_retail_price,
	SUM( COALESCE((suggested_dealer_net_price - suggested_factory_price) * demand_next_12_months_best_price, 0) ) AS profit_bu_next_12_months_best_price
    ph.harmonization,
    mppr.best_price_index_scaling AS best_price_index,
    mppr.best_price_index_category
FROM scania_parts_tab spt
JOIN (SELECT MAX(insertion_date) AS max_date FROM scania_parts_tab) AS md
    ON spt.insertion_date = md.max_date
JOIN model_prediction_prg_results mppr USING (prg, insertion_date)
LEFT JOIN prg_harmonization ph 
    ON spt.prg = ph.prg
GROUP BY 2, 3, 4, 11, 12, 13;
*/


-- Insert na tabela [scania_prg_tab] - mês selecionado

INSERT INTO scania_prg_tab (
    insertion_date,
    prg,
    cc,
    dc,
    demand_next_12_months_retail_price,
    demand_next_12_months_best_price,
    profit_sm_next_12_months_retail_price,
    profit_sm_next_12_months_best_price,
    profit_bu_next_12_months_retail_price,
    profit_bu_next_12_months_best_price,
    harmonization,
    best_price_index,
    best_price_index_category
)
SELECT
    DATE_TRUNC('month', insertion_date)::DATE AS insertion_date,
    spt.prg,
    cc,
    dc,
    SUM( COALESCE(demand_next_12_months_retail_price, 0) ) AS demand_next_12_months_retail_price,
    SUM( COALESCE(demand_next_12_months_best_price, 0) ) AS demand_next_12_months_best_price,
    SUM( COALESCE(sm_base_profit_real_calculated * demand_next_12_months_retail_price, 0) ) AS profit_sm_next_12_months_retail_price,
    SUM( COALESCE((suggested_factory_price - base_cost_price) * demand_next_12_months_best_price, 0) ) AS profit_sm_next_12_months_best_price,
    SUM( COALESCE(bu_gross_profit_real_calculated * demand_next_12_months_retail_price, 0) ) AS profit_bu_next_12_months_retail_price,
    SUM( COALESCE((suggested_dealer_net_price - suggested_factory_price) * demand_next_12_months_best_price, 0) ) AS profit_bu_next_12_months_best_price,
    ph.harmonization,
    mppr.best_price_index_scaling AS best_price_index,
    mppr.best_price_index_category
FROM scania_parts_tab spt
JOIN model_prediction_prg_results mppr USING (prg, insertion_date)
LEFT JOIN prg_harmonization ph 
    ON spt.prg = ph.prg
WHERE mppr.insertion_date >= (SELECT MAX(insertion_date) + interval '1 month' FROM scania_prg_tab)
GROUP BY 1, 2, 3, 4, 11, 12, 13;
