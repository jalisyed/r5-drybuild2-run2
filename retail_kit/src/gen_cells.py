# Cell contents for notebook 01_generate_data (Python). Each entry is one code cell.
CELLS = []

CELLS.append('''# Parameters. Change CATALOG / SCHEMA (and names in the pipeline notebook) to adapt. Everything is seeded.
import datetime, random, math
from pyspark.sql import functions as F
CATALOG, SCHEMA = "workspace", "r5_retail"
BASE = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
SEED, N_TXN, N_STORES, N_SKU, N_CUST = 42, 250_000, 60, 400, 15_000
END_DATE = datetime.date.today()
START_DATE = END_DATE - datetime.timedelta(days=729)
N_DAYS = 730
rng = random.Random(SEED)
print("range", START_DATE, END_DATE, "transactions", N_TXN)''')

CELLS.append('''# A. Reference data (run once). Small tables built in Python, written as CSV files to the volume.
CATS = [  # name, weight, price_lo, price_hi, margin_lo, margin_hi, return_rate, subcategories
 ("Grocery", .30, 2, 14, .22, .34, .010, ["Pasta", "Sauces", "Snacks", "Beverages", "Cereal"]),
 ("Fresh", .18, 3, 20, .25, .40, .015, ["Produce", "Dairy", "Meat", "Bakery"]),
 ("Apparel", .14, 12, 90, .45, .62, .060, ["Tops", "Bottoms", "Outerwear", "Kids"]),
 ("Home", .14, 6, 70, .30, .50, .030, ["Kitchen", "Cleaning", "Decor"]),
 ("Electronics", .10, 15, 400, .12, .25, .080, ["Audio", "Accessories", "Small Appliances"]),
 ("Health & Beauty", .14, 4, 40, .35, .55, .020, ["Skin Care", "Hair Care", "Wellness"])]
prod = []
for i in range(1, N_SKU + 1):
    c = rng.choices(CATS, weights=[x[1] for x in CATS])[0]
    price = round(rng.uniform(c[2], c[3]), 2); m = rng.uniform(c[4], c[5])
    prod.append(dict(idx=i, sku=f"SKU{i:05d}", category=c[0], subcategory=rng.choice(c[7]), brand=f"Brand {rng.randint(1, 25)}",
                     unit_price=price, unit_cost=round(price * (1 - m), 2), ret=c[6]))
by_group = {}
for p in prod: by_group.setdefault((p["category"], p["subcategory"]), []).append(p)
for p in prod:  # affinity partners: other items in the same subcategory (bought together)
    mates = [q["idx"] for q in by_group[(p["category"], p["subcategory"])] if q["idx"] != p["idx"]]
    p["p1"], p["p2"] = (rng.sample(mates, 2) if len(mates) >= 2 else [p["idx"], p["idx"]])
products_df = spark.createDataFrame([(p["sku"], p["category"], p["subcategory"], p["brand"], p["unit_cost"], p["unit_price"]) for p in prod],
    "sku string, category string, subcategory string, brand string, unit_cost double, unit_price double")
REG = [("Ontario", "ON", .34), ("Quebec", "QC", .22), ("British Columbia", "BC", .14), ("Prairies", "AB", .18), ("Atlantic", "NS", .12)]
stores = []
for i in range(1, N_STORES + 1):
    r = rng.choices(REG, weights=[x[2] for x in REG])[0]
    stores.append((f"S{i:03d}", f"Store {i:03d}", r[0], r[1], rng.choice(["Urban", "Suburban", "Rural"]), rng.randint(8000, 60000), str(START_DATE - datetime.timedelta(days=rng.randint(400, 6000)))))
stores_df = spark.createDataFrame(stores, "store_id string, store_name string, region string, province string, format string, size_sqft int, open_date string")
# promotions: per category, sequential non-overlapping windows; 'lift' (hidden) drives units; 6 of 30 are deliberately poor (deep discount, tiny lift)
promos, lifts, pid = [], {}, 0
for c in CATS:
    d0 = START_DATE + datetime.timedelta(days=rng.randint(10, 40))
    for k in range(5):
        pid += 1; dur = rng.randint(10, 18); poor = (pid % 5 == 0)
        disc = round(rng.uniform(.30, .40), 2) if poor else round(rng.uniform(.05, .12), 2)
        lift = round(rng.uniform(1.03, 1.10), 2) if poor else round(rng.uniform(1.9, 2.5), 2)
        promos.append((f"P{pid:03d}", f"{c[0]} promo {k + 1}", c[0], str(d0), str(d0 + datetime.timedelta(days=dur)), disc, "percent_off"))
        lifts[f"P{pid:03d}"] = lift; d0 = d0 + datetime.timedelta(days=dur + rng.randint(60, 100))
promos = [p for p in promos if datetime.date.fromisoformat(p[3]) <= END_DATE]
promotions_df = spark.createDataFrame(promos, "promo_id string, promo_name string, category string, start_date string, end_date string, discount_pct double, mechanic string")
TIERS = ["Basic", "Silver", "Gold"]
cust_df = (spark.range(1, N_CUST + 1).withColumn("customer_id", F.concat(F.lit("C"), F.lpad(F.col("id").cast("string"), 6, "0")))
  .withColumn("join_date", F.date_format(F.date_add(F.lit(str(START_DATE - datetime.timedelta(days=365))), (F.rand(SEED + 1) * (N_DAYS + 365)).cast("int")), "yyyy-MM-dd"))
  .withColumn("tier", F.element_at(F.array(*[F.lit(t) for t in TIERS]), (F.pow(F.rand(SEED + 2), 2) * 3).cast("int") + 1))
  .withColumn("province", F.element_at(F.array(*[F.lit(r[1]) for r in REG]), (F.rand(SEED + 3) * 5).cast("int") + 1))
  .withColumn("email", F.concat(F.lit("c"), F.col("id").cast("string"), F.lit("@example.test")))   # synthetic PII for governance demos
  .withColumn("phone", F.concat(F.lit("555-01"), F.lpad((F.col("id") % 100).cast("string"), 2, "0"))).drop("id"))
for name, df in [("products", products_df), ("stores", stores_df), ("promotions", promotions_df), ("customers", cust_df)]:
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{BASE}/{name}")
    print(name, df.count())''')

CELLS.append('''# B. Define the generator (safe to rerun; writes nothing). Reference data is READ BACK from the written files, never regenerated.
def read_ref(name):
    return spark.read.option("header", True).csv(f"{BASE}/{name}")
def slot_table(weights, slots):  # small lookup: slot -> index, proportional to weights (no row loops on big data)
    tot = float(sum(weights)); out, acc, idx = [], 0.0, 0
    for s in range(slots):
        while idx < len(weights) - 1 and (s + 0.5) / slots > (acc + weights[idx]) / tot: acc += weights[idx]; idx += 1
        out.append((s, idx))
    return spark.createDataFrame(out, "slot int, idx int")
def day_weights():
    w = []
    for i in range(N_DAYS):
        d = START_DATE + datetime.timedelta(days=i)
        f = {4: 1.15, 5: 1.35, 6: 1.20}.get(d.weekday(), 1.0)
        f *= {11: 1.20, 12: 1.50, 1: .85, 7: 1.05}.get(d.month, 1.0) * (1 + 0.15 * i / N_DAYS)
        w.append(f)
    return w
def generate_sales_lines(n_txn, seed):
    P = read_ref("products").withColumn("sku_idx", F.regexp_extract("sku", r"(\\d+)", 1).cast("int")) \\
        .withColumn("unit_price", F.col("unit_price").cast("double")).withColumn("unit_cost", F.col("unit_cost").cast("double"))
    pm = spark.createDataFrame([(p["idx"], p["p1"], p["p2"], p["ret"]) for p in prod], "sku_idx int, p1 int, p2 int, ret_rate double")
    C = read_ref("customers").withColumn("cid_idx", F.regexp_extract("customer_id", r"(\\d+)", 1).cast("int")).select("customer_id", "cid_idx", F.col("join_date").cast("date").alias("join_dt"))
    PR = read_ref("promotions").select("promo_id", F.col("category").alias("pcat"), F.col("start_date").cast("date").alias("ps"), F.col("end_date").cast("date").alias("pe"), F.col("discount_pct").cast("double").alias("disc_pct"))
    lift_df = spark.createDataFrame(list(lifts.items()), "promo_id string, lift double")
    days = slot_table(day_weights(), 100000).withColumnRenamed("idx", "day_idx")
    hours = slot_table([2, 3, 4, 6, 8, 9, 8, 6, 5, 6, 8, 9, 8, 5], 1000).withColumnRenamed("idx", "hour_idx")
    sw = [math.exp(rng.gauss(0, .45)) for _ in range(N_STORES)]
    sts = slot_table(sw, 2000).withColumnRenamed("idx", "store_idx")
    t = spark.range(1, n_txn + 1).withColumnRenamed("id", "tid")
    t = (t.withColumn("txn_id", F.concat(F.lit("T"), F.lpad(F.col("tid").cast("string"), 9, "0")))
      .withColumn("s_day", (F.rand(seed + 11) * 100000).cast("int")).withColumn("s_hr", (F.rand(seed + 12) * 1000).cast("int")).withColumn("s_st", (F.rand(seed + 13) * 2000).cast("int"))
      .join(F.broadcast(days), F.col("s_day") == F.col("slot")).drop("slot")
      .join(F.broadcast(hours), F.col("s_hr") == F.col("slot")).drop("slot")
      .join(F.broadcast(sts), F.col("s_st") == F.col("slot")).drop("slot"))
    t = (t.withColumn("store_id", F.concat(F.lit("S"), F.lpad((F.col("store_idx") + 1).cast("string"), 3, "0")))
      .withColumn("register_id", (F.rand(seed + 14) * 8).cast("int") + 1)
      .withColumn("event_date", F.date_add(F.lit(str(START_DATE)), F.col("day_idx")))
      .withColumn("event_ts", F.expr("timestamp(event_date) + make_interval(0, 0, 0, 0, hour_idx + 8, cast(rand(%d) * 60 as int), cast(rand(%d) * 60 as int))" % (seed + 15, seed + 16)))
      .withColumn("cand_idx", (F.pow(F.rand(seed + 17), 1.6) * N_CUST).cast("int") + 1)
      .join(F.broadcast(C), F.col("cand_idx") == F.col("cid_idx"), "left")
      .withColumn("tenure", F.datediff("event_date", "join_dt"))
      .withColumn("keep", (F.rand(seed + 18) < .80) & (F.col("tenure") >= 0) & (F.rand(seed + 19) < F.exp(-F.col("tenure") / 300.0) + .25))
      .withColumn("customer_id", F.when(F.col("keep"), F.col("customer_id")))
      .withColumn("anchor", (F.pow(F.rand(seed + 20), 1.7) * N_SKU).cast("int") + 1)
      .withColumn("n_lines", 1 + (F.pow(F.rand(seed + 21), 2.2) * 6).cast("int") + F.when(F.col("keep") & (F.rand(seed + 22) < .35), 1).otherwise(0))
      .withColumn("channel", F.when(F.rand(seed + 23) < .82, "Store").when(F.rand(seed + 24) < .67, "Online").otherwise("Click&Collect"))
      .join(F.broadcast(pm.select(F.col("sku_idx").alias("anchor"), "p1", "p2")), "anchor"))
    L = t.select("*", F.explode(F.sequence(F.lit(1), F.col("n_lines"))).alias("line_no"))
    L = (L.withColumn("sku_idx", F.when(F.col("line_no") == 1, F.col("anchor"))
            .when(F.rand(seed + 30) < .30, F.when(F.rand(seed + 31) < .5, F.col("p1")).otherwise(F.col("p2")))
            .otherwise((F.pow(F.rand(seed + 32), 1.7) * N_SKU).cast("int") + 1))
      .join(F.broadcast(P.select("sku_idx", "sku", "category", "unit_price", "unit_cost")), "sku_idx")
      .join(F.broadcast(pm.select("sku_idx", "ret_rate")), "sku_idx")
      .withColumn("promo_roll", F.rand(seed + 33))
      .join(F.broadcast(PR), (F.col("category") == F.col("pcat")) & (F.col("event_date") >= F.col("ps")) & (F.col("event_date") <= F.col("pe")) & (F.col("promo_roll") < .85), "left")
      .join(F.broadcast(lift_df), "promo_id", "left")
      .withColumn("base_qty", 1 + (F.pow(F.rand(seed + 34), 3) * 3).cast("int"))
      .withColumn("qty0", F.floor(F.col("base_qty") * F.coalesce(F.col("lift"), F.lit(1.0)) + F.rand(seed + 35)).cast("int"))
      .withColumn("is_ret", F.rand(seed + 36) < F.col("ret_rate"))
      .withColumn("qty", F.when(F.col("is_ret"), -F.col("qty0")).otherwise(F.col("qty0")))
      .withColumn("markdown", F.when((F.col("category") == "Apparel") & F.col("event_date").isNotNull() & F.month("event_date").isin(1, 7) & (F.rand(seed + 37) < .5), F.lit(0.30) + F.rand(seed + 38) * .20).otherwise(F.lit(0.0)))
      .withColumn("disc_rate", F.greatest(F.coalesce(F.col("disc_pct"), F.lit(0.0)), F.col("markdown")))
      .withColumn("discount_amt", F.round(F.col("unit_price") * F.col("qty") * F.col("disc_rate"), 2))
      .withColumn("return_reason", F.when(F.col("is_ret"), F.element_at(F.array(F.lit("defective"), F.lit("wrong size"), F.lit("changed mind"), F.lit("not as described")), (F.rand(seed + 39) * 4).cast("int") + 1)))
      .withColumn("late", F.rand(seed + 40) < .02)
      .withColumn("landing_ts", F.when(F.col("late"), F.col("event_ts") + F.expr("make_interval(0,0,0,0,6 + cast(rand(%d) * 30 as int),0,0)" % (seed + 41))).otherwise(F.col("event_ts") + F.expr("make_interval(0,0,0,0,0,cast(rand(%d) * 5 as int),cast(rand(%d) * 60 as int))" % (seed + 42, seed + 43))))
      .withColumn("sku_out", F.when(F.rand(seed + 44) < .002, F.lit("SKU99999")).otherwise(F.col("sku")))
      .withColumn("channel_out", F.when(F.rand(seed + 45) < .03, F.upper("channel")).when(F.rand(seed + 46) < .03, F.lower("channel")).otherwise(F.col("channel"))))
    out = L.select("txn_id", "line_no", "store_id", "register_id", F.date_format("event_ts", "yyyy-MM-dd HH:mm:ss").alias("event_ts"), F.date_format("landing_ts", "yyyy-MM-dd HH:mm:ss").alias("landing_ts"),
                   "customer_id", F.col("sku_out").alias("sku"), "qty", "unit_price", "discount_amt", "promo_id", F.col("channel_out").alias("channel"), "return_reason")
    dups = out.sample(False, .005, seed + 50)   # at-least-once delivery: ~0.5% of lines are re-sent
    return out.unionByName(dups)
print("generator defined")''')

CELLS.append('''# C. Write the transaction files (run once) and the weekly inventory snapshot, then print the planted patterns.
sales = generate_sales_lines(N_TXN, SEED)
sales.repartition(24).write.mode("overwrite").option("header", True).csv(f"{BASE}/sales_lines")
P2 = read_ref("products").select("sku"); S2 = read_ref("stores").select("store_id")
weeks = spark.createDataFrame([(str(START_DATE + datetime.timedelta(days=END_DATE.toordinal() - START_DATE.toordinal() - 7 * k)),) for k in range(52)], "week_start string")
inv = (S2.crossJoin(P2).where((F.abs(F.hash("store_id", "sku")) % 2) == 0).crossJoin(weeks)
  .withColumn("hot", (F.regexp_extract("sku", r"(\\d+)", 1).cast("int") <= 40) & ((F.abs(F.hash("store_id", F.lit("hot"))) % 100) < 40))
  .withColumn("stockout_days", F.when(F.col("hot") & (F.rand(SEED + 60) < .45), 1 + (F.rand(SEED + 61) * 4).cast("int")).when(~F.col("hot") & (F.rand(SEED + 62) < .03), F.lit(1)).otherwise(F.lit(0)))
  .withColumn("on_hand_end", F.when(F.col("stockout_days") >= 2, F.lit(0)).otherwise((F.pow(F.rand(SEED + 63), 2) * 120).cast("int")))
  .withColumn("on_order", (F.rand(SEED + 64) * 60).cast("int")).drop("hot"))
inv.repartition(4).write.mode("overwrite").option("header", True).csv(f"{BASE}/inventory_weekly")
chk = spark.read.option("header", True).csv(f"{BASE}/sales_lines")
print("sales line rows (incl. ~0.5% duplicates):", chk.count(), "| distinct (txn_id, line_no):", chk.select("txn_id", "line_no").distinct().count())
print("orphan sku rows:", chk.where(F.col("sku") == "SKU99999").count(), "| return lines:", chk.where(F.col("qty").cast("int") < 0).count(), "| loyalty lines:", chk.where(F.col("customer_id").isNotNull()).count())
print("inventory rows:", spark.read.option("header", True).csv(f"{BASE}/inventory_weekly").count())''')
