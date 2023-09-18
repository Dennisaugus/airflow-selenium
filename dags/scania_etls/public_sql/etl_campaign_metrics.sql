-- Insert na tabela [campaign_metrics]

INSERT INTO campaign_metrics (
    campaign_id,
    average_monthly_profit_before_campaign,
    average_monthly_profit_after_campaign,
    average_monthly_profit_variation
)
WITH profit_before_campaign AS (
    SELECT
        c.campaign_id,
        ch.month_year,
        SUM(ch.profit_dealer) AS monthly_profit
    FROM campaigns c
    JOIN campaign_history ch USING (campaign_id)
    WHERE ch.month_year < c.campaign_start_date
    AND CURRENT_DATE < c.campaign_end_date + INTERVAL '2 months'
    GROUP BY 1, 2
), profit_after_campaign AS (
    SELECT
        campaign_id,
        month_year,
        SUM(profit_dealer) AS monthly_profit
    FROM campaigns c
    JOIN campaign_history ch USING (campaign_id)
    WHERE ch.month_year >= c.campaign_start_date
    AND CURRENT_DATE < c.campaign_end_date + INTERVAL '2 months'
    GROUP BY 1, 2
)
SELECT
    c.campaign_id,
    AVG(cbp.monthly_profit) AS average_monthly_profit_before_campaign,
    AVG(cap.monthly_profit) AS average_monthly_profit_after_campaign,
    (AVG(cap.monthly_profit) / AVG(cbp.monthly_profit) - 1) AS average_monthly_profit_variation
FROM campaigns c
JOIN campaign_history ch USING (campaign_id)
JOIN profit_before_campaign cbp USING (campaign_id)
JOIN profit_after_campaign cap USING (campaign_id)
GROUP BY c.campaign_id
ON CONFLICT (campaign_id)
DO UPDATE
SET average_monthly_profit_before_campaign = EXCLUDED.average_monthly_profit_before_campaign,
    average_monthly_profit_after_campaign = EXCLUDED.average_monthly_profit_after_campaign,
    average_monthly_profit_variation = EXCLUDED.average_monthly_profit_variation;
