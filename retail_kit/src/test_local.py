import sys, shutil
sys.path.insert(0, 'src')
from pyspark.sql import SparkSession
import gen_cells
spark = SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions","8").config("spark.ui.enabled","false").getOrCreate()
shutil.rmtree("/tmp/rk_land", ignore_errors=True)
ns = {"spark": spark}
for i, c in enumerate(gen_cells.CELLS):
    c = c.replace('BASE = f"/Volumes/{CATALOG}/{SCHEMA}/landing"', 'BASE = "/tmp/rk_land"').replace("N_TXN, N_STORES, N_SKU, N_CUST = 42, 250_000", "N_TXN, N_STORES, N_SKU, N_CUST = 42, 20_000") if False else c.replace('BASE = f"/Volumes/{CATALOG}/{SCHEMA}/landing"', 'BASE = "/tmp/rk_land"').replace("250_000", "20_000").replace("15_000", "1_500")
    print("--- cell", i); exec(compile(c, f"cell{i}", "exec"), ns)
