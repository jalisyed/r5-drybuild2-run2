# Dashboard spec: "Retail performance and trust" (AI/BI dashboard)

Build the TARGET first, from the gold tables only. Three pages plus a trust tile. Every visual answers a question one persona asked. Queries are in `dataset_queries.sql` (Q01 to Q10).
Not verified on Free Edition: creating dashboards from the CLI or by Claude Code, and chart-type availability. Fallback: build the datasets, then create visuals by hand in the UI (about 20 minutes).

## Page 1: Executive view (Sally: "where is the money, and what should I act on")
| # | Visual | Source | Business question |
|---|---|---|---|
| 1 | KPI row: net sales, margin %, transactions, return rate | gold_daily_store_category, gold_margin_leakage | How is the business doing? |
| 2 | Bar: margin by category (Q01) | gold_daily_store_category | Where does profit come from? |
| 3 | Line: net sales by month with seasonality | gold_daily_store_category | What is the rhythm of demand? |
| 4 | Bar: sales and margin by region (Q04) | gold_daily_store_category | Which regions carry us? |

## Page 2: Promotions and availability (Scott: "what do we do differently")
| # | Visual | Source | Business question |
|---|---|---|---|
| 5 | Scatter: discount % vs margin change per day, coloured by verdict (Q02) | gold_promo_effectiveness | Which promotions lose money? Poor ones sit bottom-right |
| 6 | Bar: estimated lost margin from stock-outs by category (Q03) | gold_stockout_exposure | What do empty shelves cost? (label "estimate") |
| 7 | Table: top 10 worst promotions with a verdict column | gold_promo_effectiveness | Which ones do we stop? |

## Page 3: Customers and basket (analytics team)
| # | Visual | Source | Business question |
|---|---|---|---|
| 8 | Line: retention at months 0, 1, 3, 6, 12 (Q05) | gold_loyalty_cohorts | How quickly do loyalty customers drift? |
| 9 | Table: top product pairs by lift (Q06) | gold_basket_affinity | What sells together? |
| 10 | Bar: return rate and discount rate by category (Q07) | gold_margin_leakage | Where does margin leak? |
| 11 | Bar: transactions by hour (Q08) | silver_sales_lines | When is demand peaking? |

## Trust tile (Ben: "can I stand behind these numbers")
| 12 | Counter tiles: median and p95 latency, late share, duplicates removed, rows quarantined (Q09, Q10) | gold_data_trust, silver_quarantine | Is the data fresh and clean? |

## Rules
- Titles are the business question, not the table name. Each visual has a one-line "so what" subtitle.
- Use the dataviz skill's palette; one accent colour for the thing to act on, greys for the rest.
- Do not show a visual you cannot explain in 20 seconds. Cut order: 11, 9, 7, 3.
- Optional bonus: a Genie space on the gold tables (see `genie/GENIE_SETUP.md`). Verify availability first.
