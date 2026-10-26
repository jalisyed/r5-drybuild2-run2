import sys, re
sys.path.insert(0,'src')
from pyspark.sql import SparkSession
import pipeline_cells, validate_cells
spark = SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions","8").config("spark.ui.enabled","false").getOrCreate()
spark.sql("DROP DATABASE IF EXISTS rk CASCADE"); spark.sql("CREATE DATABASE rk"); spark.sql("USE rk")
def fix(sql):
    sql = sql.replace("{S}", "rk").replace("{V}", "/tmp/rk_land")
    sql = re.sub(r"read_files\('/tmp/rk_land/(\w+)'.*?\)", lambda m: f"src_{m.group(1)}", sql)
    return sql.replace("_metadata.file_path", "'file' ")
for n in ["sales_lines","inventory_weekly","products","stores","customers","promotions"]:
    spark.read.option("header",True).csv(f"/tmp/rk_land/{n}").createOrReplaceTempView(f"src_{n}")
for t, s in pipeline_cells.CELLS:
    q=fix(s); m=re.match(r'CREATE OR REPLACE TABLE (\S+) AS',q)
    if m: spark.sql('DROP TABLE IF EXISTS '+m.group(1)); q=q.replace('CREATE OR REPLACE TABLE','CREATE TABLE',1)
    spark.sql(q); print("ok", t[:60])
for t, s in validate_cells.CELLS:
    print("\n##", t); spark.sql(fix(s)).show(14, truncate=False)
import prompts
print("\n===== PROMPT LIBRARY =====")
for p in prompts.PROMPTS:
    n = spark.sql(fix(p["sql"])).count(); print(p["id"], "rows", n)
