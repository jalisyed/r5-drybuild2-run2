# retail_kit: synthetic retail data, pipeline, dashboard spec and prompt library

Reusable for a pre-sales demo in the retail vertical. **All data is synthetic.** Tested locally in Spark (PySpark 4.2, reduced scale: 20k transactions). **Not yet run on Databricks**; run order and checks are below.

## Run order (Databricks Free Edition, serverless)
1. Import the four notebooks in `notebooks/` (or the repo as a Git folder).
2. `00_setup` (SQL): creates `workspace.r5_retail` and the `landing` volume.
3. `01_generate_data` (Python): run cells top to bottom once. Full scale is 250k transactions (about 800k sales lines), 60 stores, 400 SKUs, 15k loyalty customers, 730 days ending today. If a cell is slow, lower `N_TXN` in the first cell.
4. `02_pipeline` (SQL): bronze, silver, gold. Every cell is `CREATE OR REPLACE`, safe to rerun.
5. `03_validate_results` (SQL): compare outputs with the patterns below.
6. `sql/grants.sql` after every pipeline rerun if an app reads the tables.
Choose schema names when building the notebooks: `python tools/make_nb.py --schema r5_build --data-schema r5_data` (needs `nbformat`). `--schema` holds the pipeline tables; `--data-schema` holds the shared `landing` volume with the source files. Two runs (dry run and interview) can share one data schema and use separate table schemas. Without arguments everything uses `r5_retail`. Notebook 01 writes the shared source files: run it once only.

## Tables
| Layer | Table | Purpose |
|---|---|---|
| landing (files) | products, stores, customers, promotions, sales_lines, inventory_weekly | CSV files in the volume |
| bronze | bronze_sales_lines, bronze_inventory_weekly | raw strings plus file lineage |
| silver | dim_product, dim_store, dim_customer, dim_promotion, dim_date, silver_sales_lines, silver_quarantine | typed, de-duplicated, standardised; bad rows kept in quarantine |
| gold | gold_daily_store_category, gold_promo_effectiveness, gold_stockout_exposure, gold_basket_affinity, gold_loyalty_cohorts, gold_margin_leakage, gold_data_trust | one per business question |

## Planted business patterns (what the story is built on)
| Pattern | How it is planted | Where it shows |
|---|---|---|
| Seasonality | December +50%, November +20%, January -15%; weekend peak; 15% growth over two years | gold_daily_store_category by month |
| Promotions that lose money | 30 promotions; 6 (the fifth of each category) have 30 to 40% discount and almost no lift | gold_promo_effectiveness: verdict "margin dilutive" (expect 6 to 8) |
| Stock-outs on fast movers | top-40 SKUs in about 40% of stores are out 1 to 4 days in many weeks | gold_stockout_exposure (an estimate, labelled so) |
| Margin leakage | return rates by category (Electronics highest); Apparel markdowns of 30 to 50% in January and July | gold_margin_leakage |
| Basket affinity | 30% of extra lines are a partner product from the same subcategory | gold_basket_affinity lift well above 1 |
| Loyalty decay | loyalty tag fades with tenure | gold_loyalty_cohorts |
| Data quality | about 0.5% duplicate lines, about 2% lines landing 6 or more hours late, 0.2% unknown SKUs, channel casing noise | gold_data_trust, silver_quarantine |

## Caveats to say out loud
- Stock-out loss is an estimate. Sales are not generated to dip during a stock-out, so do not claim a measured effect.
- Customer email and phone are synthetic placeholders for a governance conversation (masking), not real data.
- Numbers in the validate notebook come from this seeded data. Quote only numbers you recomputed after the run.

## Files
`notebooks/` four notebooks; `src/` cell sources and local tests (`test_local.py`, `test_pipeline.py` need pyspark); `dashboards/DASHBOARD_SPEC.md` and `dataset_queries.sql`; `genie/GENIE_SETUP.md`; `app/` Streamlit prompt library (`prompts.json` generated); `sql/grants.sql`; `tools/make_nb.py`.
