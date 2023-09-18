-- Insert na tabela [price_list] - dados mensais

INSERT INTO price_list (
	part_number,
	part_description,
	cc,
    dc,
	prg,
	origin,
    part_status,
	demand_last_12_months,
    month_year,
	base_cost_price,
    current_factory_price,
	current_dealer_net_price,
	current_dealer_price,
	current_retail_price,
    cofins_percentage,
    pis_percentage,
    piscofins_real,
    ipi_percentage,
    ipi_real,
	icms_percentage,
	icms_real,
    current_sm_margin,
    current_bubr_margin,
    current_dealer_margin,
	rrp_real,
    part_cost,
    sm_base_profit_real,
    bu_gross_profit_real_calculated,
    dealer_profit_real,
    sm_base_profit_real_calculated
)
SELECT
    rpls.part_number,
    rpls.part_description,
    SUBSTRING (rpls.cc_dc,1,1) AS cc,
    CAST(SUBSTRING(rpls.cc_dc,2,1) AS INTEGER) AS dc,
    rpls.prg,
    rpls.origin,
    rpls.part_status,
    rpls.demand_last_12_months,
    rpls.month_year,
    rpls.base_cost_real,
    rpls.factory_price_real,
    rplb.dealer_net,
    rplb.dealer_price,
    rplb.shelf_price AS retail_price,
    rplb.cofins,
    rplb.pis,
    rplb.piscofins_real,
    rplb.ipi,
    rplb.ipi_real,
    rplb.icms,
    rplb.icms_real,
    rpls.sm_base_margin AS current_sm_margin,
    rplb.margin AS current_bubr_margin,
    rplb.contribution_margin_pct AS current_dealer_margin,
    rpls.gross_price_usd AS rrp_real,
    rplb.part_cost,
    rpls.sm_base_profit_real,
    (rplb.dealer_net - rplb.fprice_real) AS bu_gross_profit_real_calculated,
    rplb.contribution_margin_real AS dealer_profit_real,
    (rplb.fprice_real - rpls.base_cost_real) AS sm_base_profit_calculated
FROM raw_data.raw_price_list_sm rpls
JOIN raw_data.raw_price_list_bu rplb
USING (part_number, month_year)
WHERE SUBSTRING(rpls.cc_dc,1,1) != '0'
    AND SUBSTRING(rpls.cc_dc,2,1) IS NOT NULL
    AND rpls.base_cost_real > 0
    AND rpls.factory_price_real > 0
    AND rpls.month_year > (SELECT MAX(month_year) FROM public.price_list);


-- Insert na tabela [price_list] - todos os meses
/*
INSERT INTO price_list (
    part_number,
    part_description,
    cc,
    dc,
    prg,
    origin,
    part_status,
    demand_last_12_month,
    month_year,
    base_cost_price,
    current_factory_price,
    current_dealer_net_price,
    current_dealer_price,
    current_retail_price,
    cofins_percentage,
    pis_percentage,
    piscofins_real,
    ipi_percentage,
    ipi_real,
    icms_percentage,
    icms_real,
    current_sm_margin,
    current_bubr_margin,
    current_dealer_margin,
    rrp_real,
    part_cost,
    sm_base_profit_real,
    bu_gross_profit_real_calculated,
    dealer_profit_real,
    sm_base_profit_real_calculated
)
SELECT
    rpls.part_number,
    rpls.part_description,
    SUBSTRING (rpls.cc_dc,1,1) AS cc,
    CAST(SUBSTRING(rpls.cc_dc,2,1) AS INTEGER) AS dc,
    rpls.prg,
    rpls.origin,
    rpls.part_status,
    rpls.demand_last_12_month,
    rpls.month_year,
    rpls.base_cost_real,
    rpls.factory_price_real,
    rplb.dealer_net,
    rplb.dealer_price,
    rplb.shelf_price AS retail_price,
    rplb.cofins,
    rplb.pis,
    rplb.piscofins_real,
    rplb.ipi,
    rplb.ipi_real,
    rplb.icms,
    rplb.icms_real,
    rpls.sm_base_margin AS current_sm_margin,
    rplb.margin AS current_bubr_margin,
    rplb.contribution_margin_pct AS current_dealer_margin,
    rpls.gross_price_usd AS rrp_real,
    rplb.part_cost,
    rpls.sm_base_profit_real,
    (rplb.dealer_net - rplb.fprice_real) AS bu_gross_profit_real_calculated,
    rplb.contribution_margin_real AS dealer_profit_real,
    (rplb.fprice_real - rpls.base_cost_real) AS sm_base_profit_calculated
FROM raw_data.raw_price_list_sm rpls
JOIN (SELECT MAX(month_year) AS max_month_year FROM raw_data.raw_price_list_sm) AS my
    ON rpls.month_year = my.max_month_year
JOIN raw_data.raw_price_list_bu rplb
    ON rpls.part_number = rplb.part_number
    AND rpls.month_year = rplb.month_year
WHERE SUBSTRING(rpls.cc_dc,1,1) != '0'
    AND SUBSTRING(rpls.cc_dc,2,1) IS NOT NULL
    AND rpls.base_cost_real > 0
    AND rpls.factory_price_real > 0;
*/
