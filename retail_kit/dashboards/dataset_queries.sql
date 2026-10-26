-- Dataset queries for the AI/BI dashboard (one per persona question). Generated from src/prompts.py

-- Q01 [Executive] Which categories earn our margin, and how is each trending?
SELECT category, round(sum(net_sales),0) AS net_sales, round(sum(margin),0) AS margin, round(sum(margin)/sum(net_sales),3) AS margin_rate FROM workspace.r5_drybuild2.gold_daily_store_category GROUP BY category ORDER BY margin DESC;

-- Q02 [Executive] Which promotions lost us money, even though they sold more?
SELECT promo_name, discount_pct, round(lift,2) AS volume_lift, round(margin_change_per_day,0) AS margin_change_per_day, verdict FROM workspace.r5_drybuild2.gold_promo_effectiveness ORDER BY margin_change_per_day LIMIT 10;

-- Q03 [Executive] Where are stock-outs costing us the most (estimated)?
SELECT category, count(*) AS store_sku_weeks_out, round(sum(est_lost_units),0) AS est_lost_units, round(sum(est_lost_margin),0) AS est_lost_margin FROM workspace.r5_drybuild2.gold_stockout_exposure GROUP BY category ORDER BY est_lost_margin DESC;

-- Q04 [Executive] How does sales compare by region?
SELECT region, round(sum(net_sales),0) AS net_sales, round(sum(margin),0) AS margin, sum(transactions) AS transactions FROM workspace.r5_drybuild2.gold_daily_store_category GROUP BY region ORDER BY net_sales DESC;

-- Q05 [Business analytics] How many loyalty customers are still buying 1, 3, 6 and 12 months after their first purchase?
WITH c0 AS (SELECT cohort_month, active_customers AS n0 FROM workspace.r5_drybuild2.gold_loyalty_cohorts WHERE months_since = 0) SELECT g.months_since, round(avg(g.active_customers / c0.n0),3) AS share_still_buying FROM workspace.r5_drybuild2.gold_loyalty_cohorts g JOIN c0 ON g.cohort_month = c0.cohort_month WHERE g.months_since IN (0,1,3,6,12) GROUP BY g.months_since ORDER BY g.months_since;

-- Q06 [Business analytics] Which products are bought together most often?
SELECT sku_a, sku_b, together, round(lift,1) AS lift FROM workspace.r5_drybuild2.gold_basket_affinity ORDER BY lift DESC LIMIT 10;

-- Q07 [Business analytics] Where do returns and discounts eat into sales?
SELECT category, round(sum(returns_value)/sum(gross_net_sales),3) AS return_rate_value, round(sum(discount_given)/(sum(gross_net_sales)+sum(discount_given)),3) AS discount_rate FROM workspace.r5_drybuild2.gold_margin_leakage GROUP BY category ORDER BY return_rate_value DESC;

-- Q08 [Business analytics] When during the day do we sell the most?
SELECT hour(event_ts) AS hour_of_day, count(DISTINCT txn_id) AS transactions, round(sum(net_sales),0) AS net_sales FROM workspace.r5_drybuild2.silver_sales_lines GROUP BY hour(event_ts) ORDER BY hour_of_day;

-- Q09 [Data engineering] How fresh and complete is the data behind these numbers?
SELECT bronze_rows, silver_rows, quarantined_rows, duplicates_removed_est, round(median_latency_min,1) AS median_latency_min, round(p95_latency_min,1) AS p95_latency_min, round(late_share,4) AS late_share, latest_event_ts FROM workspace.r5_drybuild2.gold_data_trust;

-- Q10 [Data engineering] Which rows were rejected and why?
SELECT reason, count(*) AS rows, count(DISTINCT store_id) AS stores_affected FROM workspace.r5_drybuild2.silver_quarantine GROUP BY reason;

