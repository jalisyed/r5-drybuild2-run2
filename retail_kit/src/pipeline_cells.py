# SQL cells for notebook 02_pipeline. {S} = catalog.schema, {V} = volume path. One statement per cell.
RF = "read_files('{V}/%s', format => 'csv', header => true)"
CELLS = []
def add(title, sql): CELLS.append((title, sql.strip()))

add("Bronze: land raw files as-is (all strings) with lineage columns", f"""
CREATE OR REPLACE TABLE {{S}}.bronze_sales_lines AS
SELECT *, _metadata.file_path AS source_file, current_timestamp() AS ingested_at FROM {RF % 'sales_lines'}""")
add("Bronze: weekly inventory snapshot", f"""
CREATE OR REPLACE TABLE {{S}}.bronze_inventory_weekly AS
SELECT *, _metadata.file_path AS source_file FROM {RF % 'inventory_weekly'}""")

add("Dimensions: product (typed)", f"""
CREATE OR REPLACE TABLE {{S}}.dim_product AS
SELECT sku, category, subcategory, brand, try_cast(unit_cost AS DOUBLE) AS unit_cost, try_cast(unit_price AS DOUBLE) AS unit_price
FROM {RF % 'products'}""")
add("Dimensions: store", f"""
CREATE OR REPLACE TABLE {{S}}.dim_store AS
SELECT store_id, store_name, region, province, format, try_cast(size_sqft AS INT) AS size_sqft, try_cast(open_date AS DATE) AS open_date
FROM {RF % 'stores'}""")
add("Dimensions: customer (synthetic PII columns kept on purpose, masked in the governance story)", f"""
CREATE OR REPLACE TABLE {{S}}.dim_customer AS
SELECT customer_id, try_cast(join_date AS DATE) AS join_date, tier, province, email, phone
FROM {RF % 'customers'}""")
add("Dimensions: promotion", f"""
CREATE OR REPLACE TABLE {{S}}.dim_promotion AS
SELECT promo_id, promo_name, category, try_cast(start_date AS DATE) AS start_date, try_cast(end_date AS DATE) AS end_date,
       try_cast(discount_pct AS DOUBLE) AS discount_pct, mechanic
FROM {RF % 'promotions'}""")
add("Dimensions: date", """
CREATE OR REPLACE TABLE {S}.dim_date AS
SELECT d AS date, year(d) AS year, month(d) AS month, date_format(d, 'yyyy-MM') AS year_month, weekofyear(d) AS week,
       date_format(d, 'E') AS dow, dayofweek(d) IN (1, 7) AS is_weekend
FROM (SELECT explode(sequence((SELECT min(try_cast(event_ts AS DATE)) FROM {S}.bronze_sales_lines),
                              (SELECT max(try_cast(event_ts AS DATE)) FROM {S}.bronze_sales_lines), interval 1 day)) AS d)""")

add("Silver: typed, de-duplicated, standardised sales lines (good rows only)", """
CREATE OR REPLACE TABLE {S}.silver_sales_lines AS
WITH typed AS (
  SELECT txn_id, try_cast(line_no AS INT) AS line_no, store_id, try_cast(register_id AS INT) AS register_id,
         try_cast(event_ts AS TIMESTAMP) AS event_ts, try_cast(landing_ts AS TIMESTAMP) AS landing_ts,
         customer_id, sku, try_cast(qty AS INT) AS qty, try_cast(discount_amt AS DOUBLE) AS discount_amt,
         promo_id, initcap(channel) AS channel, return_reason
  FROM {S}.bronze_sales_lines),
dedup AS (
  SELECT *, row_number() OVER (PARTITION BY txn_id, line_no ORDER BY landing_ts) AS rn FROM typed)
SELECT d.txn_id, d.line_no, d.store_id, d.register_id, d.event_ts, CAST(d.event_ts AS DATE) AS event_date, d.landing_ts,
       d.customer_id, d.sku, p.category, p.subcategory, d.qty, p.unit_price, p.unit_cost, d.discount_amt, d.promo_id, d.channel, d.return_reason,
       d.qty * p.unit_price - d.discount_amt AS net_sales, d.qty * p.unit_cost AS cogs,
       (d.qty * p.unit_price - d.discount_amt) - d.qty * p.unit_cost AS margin,
       (unix_timestamp(d.landing_ts) - unix_timestamp(d.event_ts)) / 60.0 AS latency_min
FROM dedup d JOIN {S}.dim_product p ON d.sku = p.sku
WHERE d.rn = 1 AND d.qty IS NOT NULL AND d.event_ts IS NOT NULL""")
add("Silver: quarantine (rows that failed a rule, kept, never silently dropped)", """
CREATE OR REPLACE TABLE {S}.silver_quarantine AS
SELECT b.txn_id, b.line_no, b.sku, b.store_id, 'unknown_sku' AS reason
FROM {S}.bronze_sales_lines b LEFT ANTI JOIN {S}.dim_product p ON b.sku = p.sku""")

add("Gold: daily store x category (feeds the Executive page)", """
CREATE OR REPLACE TABLE {S}.gold_daily_store_category AS
SELECT s.event_date, s.store_id, st.region, s.category, s.channel,
       sum(s.net_sales) AS net_sales, sum(s.margin) AS margin, sum(s.qty) AS units, sum(s.discount_amt) AS discount_amt,
       count(DISTINCT s.txn_id) AS transactions
FROM {S}.silver_sales_lines s JOIN {S}.dim_store st ON s.store_id = st.store_id
GROUP BY s.event_date, s.store_id, st.region, s.category, s.channel""")

add("Gold: promotion effectiveness (lift vs the 28 days before, margin after discount)", """
CREATE OR REPLACE TABLE {S}.gold_promo_effectiveness AS
WITH win AS (
  SELECT p.promo_id, p.promo_name, p.category, p.start_date, p.end_date, p.discount_pct,
         datediff(p.end_date, p.start_date) + 1 AS promo_days
  FROM {S}.dim_promotion p),
promo_units AS (
  SELECT w.promo_id, sum(s.qty) AS promo_units, sum(s.margin) AS promo_margin, sum(s.discount_amt) AS promo_discount
  FROM win w JOIN {S}.silver_sales_lines s ON s.category = w.category AND s.event_date BETWEEN w.start_date AND w.end_date
  GROUP BY w.promo_id),
base_units AS (
  SELECT w.promo_id, sum(s.qty) AS base_units, sum(s.margin) AS base_margin
  FROM win w JOIN {S}.silver_sales_lines s ON s.category = w.category AND s.event_date BETWEEN date_sub(w.start_date, 28) AND date_sub(w.start_date, 1)
  GROUP BY w.promo_id)
SELECT w.promo_id, w.promo_name, w.category, w.start_date, w.discount_pct, w.promo_days,
       pu.promo_units / w.promo_days AS units_per_day, bu.base_units / 28.0 AS base_units_per_day,
       (pu.promo_units / w.promo_days) / nullif(bu.base_units / 28.0, 0) AS lift,
       pu.promo_margin / w.promo_days AS margin_per_day, bu.base_margin / 28.0 AS base_margin_per_day,
       (pu.promo_margin / w.promo_days) - (bu.base_margin / 28.0) AS margin_change_per_day,
       CASE WHEN (pu.promo_margin / w.promo_days) < (bu.base_margin / 28.0) THEN 'margin dilutive' ELSE 'margin accretive' END AS verdict
FROM win w JOIN promo_units pu ON w.promo_id = pu.promo_id JOIN base_units bu ON w.promo_id = bu.promo_id""")

add("Gold: stock-out exposure (ESTIMATE: expected weekly units x days out of stock x unit margin)", """
CREATE OR REPLACE TABLE {S}.gold_stockout_exposure AS
WITH wk AS (
  SELECT store_id, sku, try_cast(week_start AS DATE) AS week_start, try_cast(stockout_days AS INT) AS stockout_days,
         try_cast(on_hand_end AS INT) AS on_hand_end, try_cast(on_order AS INT) AS on_order
  FROM {S}.bronze_inventory_weekly),
run AS (
  SELECT store_id, sku, sum(qty) / 104.0 AS avg_weekly_units, avg(margin / nullif(qty, 0)) AS unit_margin
  FROM {S}.silver_sales_lines WHERE qty > 0 GROUP BY store_id, sku)
SELECT w.store_id, w.sku, p.category, w.week_start, w.stockout_days,
       r.avg_weekly_units * w.stockout_days / 7.0 AS est_lost_units,
       r.avg_weekly_units * w.stockout_days / 7.0 * coalesce(r.unit_margin, 0) AS est_lost_margin
FROM wk w JOIN {S}.dim_product p ON w.sku = p.sku LEFT JOIN run r ON w.store_id = r.store_id AND w.sku = r.sku
WHERE w.stockout_days > 0""")

add("Gold: basket affinity (pairs bought together, lift over chance)", """
CREATE OR REPLACE TABLE {S}.gold_basket_affinity AS
WITH items AS (SELECT DISTINCT txn_id, sku FROM {S}.silver_sales_lines WHERE qty > 0),
n AS (SELECT count(DISTINCT txn_id) AS total FROM items),
single AS (SELECT sku, count(*) AS cnt FROM items GROUP BY sku),
pairs AS (
  SELECT a.sku AS sku_a, b.sku AS sku_b, count(*) AS together
  FROM items a JOIN items b ON a.txn_id = b.txn_id AND a.sku < b.sku GROUP BY a.sku, b.sku HAVING count(*) >= 15)
SELECT p.sku_a, p.sku_b, p.together, p.together / n.total AS support,
       (p.together / n.total) / ((sa.cnt / n.total) * (sb.cnt / n.total)) AS lift
FROM pairs p CROSS JOIN n JOIN single sa ON p.sku_a = sa.sku JOIN single sb ON p.sku_b = sb.sku""")

add("Gold: loyalty cohorts (first purchase month -> months since -> customers still buying)", """
CREATE OR REPLACE TABLE {S}.gold_loyalty_cohorts AS
WITH first AS (
  SELECT customer_id, date_trunc('month', min(event_date)) AS cohort_month
  FROM {S}.silver_sales_lines WHERE customer_id IS NOT NULL AND qty > 0 GROUP BY customer_id),
act AS (
  SELECT DISTINCT s.customer_id, date_trunc('month', s.event_date) AS m
  FROM {S}.silver_sales_lines s WHERE s.customer_id IS NOT NULL AND s.qty > 0)
SELECT f.cohort_month, CAST(months_between(a.m, f.cohort_month) AS INT) AS months_since, count(DISTINCT a.customer_id) AS active_customers
FROM first f JOIN act a ON f.customer_id = a.customer_id GROUP BY f.cohort_month, CAST(months_between(a.m, f.cohort_month) AS INT)""")

add("Gold: margin leakage (returns and markdowns by category and month)", """
CREATE OR REPLACE TABLE {S}.gold_margin_leakage AS
SELECT date_trunc('month', event_date) AS month, category,
       sum(CASE WHEN qty > 0 THEN net_sales ELSE 0 END) AS gross_net_sales,
       -sum(CASE WHEN qty < 0 THEN net_sales ELSE 0 END) AS returns_value,
       sum(CASE WHEN qty > 0 THEN discount_amt ELSE 0 END) AS discount_given,
       sum(margin) AS net_margin
FROM {S}.silver_sales_lines GROUP BY date_trunc('month', event_date), category""")

add("Gold: data trust metrics (freshness, late data, duplicates removed, quarantined rows)", """
CREATE OR REPLACE TABLE {S}.gold_data_trust AS
SELECT (SELECT count(*) FROM {S}.bronze_sales_lines) AS bronze_rows,
       (SELECT count(*) FROM {S}.silver_sales_lines) AS silver_rows,
       (SELECT count(*) FROM {S}.silver_quarantine) AS quarantined_rows,
       (SELECT count(*) FROM {S}.bronze_sales_lines) - (SELECT count(*) FROM {S}.silver_sales_lines) - (SELECT count(*) FROM {S}.silver_quarantine) AS duplicates_removed_est,
       (SELECT percentile_approx(latency_min, 0.5) FROM {S}.silver_sales_lines) AS median_latency_min,
       (SELECT percentile_approx(latency_min, 0.95) FROM {S}.silver_sales_lines) AS p95_latency_min,
       (SELECT avg(CASE WHEN latency_min > 360 THEN 1.0 ELSE 0.0 END) FROM {S}.silver_sales_lines) AS late_share,
       (SELECT max(event_ts) FROM {S}.silver_sales_lines) AS latest_event_ts""")
