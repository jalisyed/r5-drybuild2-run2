"""Build the .ipynb notebooks and app/prompts.json from the cell sources in src/. Run: python tools/make_nb.py"""
import json, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import nbformat as nbf
import gen_cells, pipeline_cells, validate_cells, prompts
import argparse
ap = argparse.ArgumentParser(); ap.add_argument("--schema", default="r5_retail", help="schema for pipeline tables (r5_drybuild2 or r5_build)")
ap.add_argument("--data-schema", default=None, help="schema holding the landing volume with the shared source files (default: same as --schema)")
a = ap.parse_args(); DS = a.data_schema or a.schema
S, V = f"workspace.{a.schema}", f"/Volumes/workspace/{DS}/landing"
root = os.path.join(os.path.dirname(__file__), "..")
def nb(cells, lang_note):
    n = nbf.v4.new_notebook(); n.cells = cells
    n.metadata["kernelspec"] = {"display_name": "Python 3", "language": "python", "name": "python3"}
    n.metadata["language_info"] = {"name": "python"}
    nbf.validate(n); return n
def md(t): return nbf.v4.new_markdown_cell(t)
def code(t): return nbf.v4.new_code_cell(t)
def sql(t): return nbf.v4.new_code_cell("%sql\n" + t)

# 00 setup
c = [md("# 00 Setup\nCreates the schema and the landing volume. Safe to rerun. The catalog is `workspace` (Free Edition default); change `S` below everywhere if yours differs."),
     sql(f"CREATE SCHEMA IF NOT EXISTS workspace.{DS}"),
     sql(f"CREATE VOLUME IF NOT EXISTS workspace.{DS}.landing"),
     sql(f"CREATE SCHEMA IF NOT EXISTS workspace.{a.schema}")]
nbf.write(nb(c, "sql"), os.path.join(root, "notebooks", "00_setup.ipynb"))

# 01 generate
titles = ["Parameters", "A. Reference data (run once)", "B. Define the generator", "C. Write transaction files and inventory (run once)"]
c = [md("# 01 Generate synthetic retail data (Python / PySpark)\nSeeded, built-ins only, Spark Connect safe. Writes CSV files to the landing volume; the pipeline notebook ingests them. All data is synthetic.\n\n**Run order:** cells top to bottom, once. Rerunning cell A or C overwrites the files (idempotent).\n\n**Planted patterns** (see README): seasonality and weekend peaks, promotion lift with 6 deliberately poor promotions, stock-out windows on fast-moving items, return and markdown leakage, loyalty baskets with partner products, cohort decay, ~0.5% duplicate lines, ~2% late-arriving lines, a few orphan SKUs, inconsistent channel casing.")]
for t, s in zip(titles, gen_cells.CELLS):
    s = s.replace('CATALOG, SCHEMA = "workspace", "r5_retail"', f'CATALOG, SCHEMA = "workspace", "{DS}"')
    c += [md("### " + t), code(s)]
nbf.write(nb(c, "python"), os.path.join(root, "notebooks", "01_generate_data.ipynb"))

# 02 pipeline
c = [md("# 02 Pipeline (SQL): bronze, silver, gold\nLean medallion: raw landing (bronze), typed and de-duplicated (silver), business tables (gold). Every gold table exists to answer a named business question. `CREATE OR REPLACE` makes every cell safe to rerun.")]
for t, s in pipeline_cells.CELLS:
    c += [md("### " + t), sql(s.replace("{S}", S).replace("{V}", V))]
nbf.write(nb(c, "sql"), os.path.join(root, "notebooks", "02_pipeline.ipynb"))

# 03 validate
c = [md("# 03 Validate results\nBusiness-readable checks. Read the output against the planted patterns in the README before the dashboard is built. If a number looks wrong, fix the data or the pipeline, not the slide.")]
for t, s in validate_cells.CELLS:
    c += [md("### " + t), sql(s.replace("{S}", S))]
nbf.write(nb(c, "sql"), os.path.join(root, "notebooks", "03_validate_results.ipynb"))

# prompt library for the app (SQL resolved to the real schema)
lib = [dict(id=p["id"], persona=p["persona"], question=p["q"], why=p["why"], sql=p["sql"].replace("{S}", S)) for p in prompts.PROMPTS]
json.dump(lib, open(os.path.join(root, "app", "prompts.json"), "w"), indent=2)
# plain SQL of the gold layer for dashboard datasets
with open(os.path.join(root, "dashboards", "dataset_queries.sql"), "w") as f:
    f.write("-- Dataset queries for the AI/BI dashboard (one per persona question). Generated from src/prompts.py\n\n")
    for p in lib: f.write(f"-- {p['id']} [{p['persona']}] {p['question']}\n{p['sql']};\n\n")
open(os.path.join(root, "sql", "grants.sql"), "w").write(open(os.path.join(root, "src", "grants.template.sql")).read().replace("workspace.r5_retail", S))
print("built", [x for x in os.listdir(os.path.join(root, 'notebooks'))])
