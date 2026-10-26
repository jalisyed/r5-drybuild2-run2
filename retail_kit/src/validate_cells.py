# SQL cells for notebook 03_validate_results. {S} = catalog.schema. Each is a business-readable check.
CELLS = [
("Row accounting: bronze = silver + quarantine + duplicates removed", "SELECT * FROM {S}.gold_data_trust"),
("Promotions: how many are margin dilutive, and the worst few", "SELECT verdict, count(*) AS promos, round(avg(lift), 2) AS avg_lift, round(avg(discount_pct), 2) AS avg_discount FROM {S}.gold_promo_effectiveness GROUP BY verdict"),
("Promotions detail (worst first)", "SELECT promo_name, discount_pct, round(lift, 2) AS lift, round(margin_change_per_day, 0) AS margin_change_per_day, verdict FROM {S}.gold_promo_effectiveness ORDER BY margin_change_per_day LIMIT 8"),
("Stock-outs: estimated exposure by category", "SELECT category, count(*) AS store_sku_weeks, round(sum(est_lost_units), 0) AS est_lost_units, round(sum(est_lost_margin), 0) AS est_lost_margin FROM {S}.gold_stockout_exposure GROUP BY category ORDER BY est_lost_margin DESC"),
("Basket affinity: top pairs by lift", "SELECT sku_a, sku_b, together, round(lift, 1) AS lift FROM {S}.gold_basket_affinity ORDER BY lift DESC LIMIT 5"),
("Loyalty: average retention curve (share of cohort still buying)", "WITH c0 AS (SELECT cohort_month, active_customers AS n0 FROM {S}.gold_loyalty_cohorts WHERE months_since = 0) SELECT g.months_since, round(avg(g.active_customers / c0.n0), 3) AS retention FROM {S}.gold_loyalty_cohorts g JOIN c0 ON g.cohort_month = c0.cohort_month WHERE g.months_since IN (0, 1, 3, 6, 12) GROUP BY g.months_since ORDER BY g.months_since"),
("Leakage: returns and discounts as share of sales, by category", "SELECT category, round(sum(returns_value) / sum(gross_net_sales), 3) AS return_rate_value, round(sum(discount_given) / (sum(gross_net_sales) + sum(discount_given)), 3) AS discount_rate FROM {S}.gold_margin_leakage GROUP BY category ORDER BY return_rate_value DESC"),
("Seasonality: sales by month (peak should be December)", "SELECT date_format(event_date, 'yyyy-MM') AS ym, round(sum(net_sales), 0) AS net_sales FROM {S}.gold_daily_store_category GROUP BY 1 ORDER BY 1"),
("Quarantine reasons", "SELECT reason, count(*) AS rows FROM {S}.silver_quarantine GROUP BY reason"),
]
