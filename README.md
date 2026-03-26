        ┌──────────────────────────────┐
        │   Azure Blob Storage (CSV)   │
        │   Raw Stock Data             │
        └──────────────┬───────────────┘
                       │
                       ▼
        ┌──────────────────────────────┐
        │   Data Ingestion (PySpark)   │
        │   - Read CSV                 │
        │   - Apply Schema             │
        └──────────────┬───────────────┘
                       │
                       ▼
        ┌──────────────────────────────┐
        │ Data Validation & Cleaning   │
        │ - Column standardization     │
        │ - Handle missing values      │
        │ - Deduplication (Window Fn)  │
        └──────────────┬───────────────┘
                       │
                       ▼
        ┌──────────────────────────────┐
        │ Feature Engineering          │
        │ - Daily Return               │
        │ - Daily Return %             │
        │ - Volatility (7/14/30)       │
        └──────────────┬───────────────┘
                       │
                       ▼
        ┌──────────────────────────────┐
        │ Aggregation Layer            │
        │ - Daily                      │
        │ - Weekly                     │
        │ - Monthly                    │
        │ - avg, max, min, sum         │
        └──────────────┬───────────────┘
                       │
                       ▼
        ┌──────────────────────────────┐
        │ Top Movers                   │
        │ - High Return % (Gainers)   │
        │ - Low Return % (Losers)     │
        └──────────────┬───────────────┘
                       │
                       ▼
        ┌──────────────────────────────┐
        │   Gold Layer (Delta Tables)  │
        │ - Daily Table                │
        │ - Weekly Table               │
        │ - Monthly Table              │
        │ - Movers Table               │
        └──────────────┬───────────────┘
                       │
                       ▼
        ┌──────────────────────────────┐
        │                              │
        │   DataBricks DashBoard       │
        └──────────────────────────────┘
------------------------------------------------------------------------------------------------------------------
Stock Market Data Engineering Pipeline (PySpark + Delta Lake)

I built a scalable data pipeline to process stock market data—from raw ingestion to polished, analysis-ready datasets. The pipeline leverages PySpark for distributed processing and Delta Lake for reliable, ACID-compliant storage. It handles everything from data cleaning and transformation to feature engineering and aggregation.

1. Ingesting Data (Schema-Based)

The pipeline pulls CSV files from Azure Blob Storage. Instead of relying on automatic schema inference, I defined a custom schema to ensure consistent and accurate data types while improving performance.

Key approach:

Defined schema using StructType and StructField.
Read CSV with the schema applied:
spark.read.csv(..., header=True, schema=schema)

2. Validating & Standardizing Columns

Before doing any heavy processing, I made sure all the expected columns exist. Missing columns are handled dynamically, and column names are sanitized to be SQL-friendly (lowercase, no special characters).

Techniques used:

Looping through expected columns and adding missing ones using lit(None).
Renaming columns with withColumnRenamed() for consistency.

3. Cleaning & Deduplicating Data

Stock data often has duplicates or missing values. To handle this:

Duplicates were removed using window functions, keeping the latest or highest closing price per sector and date.
Forward-filled missing values where appropriate.

Example:

windowSpec = Window.partitionBy("date", "sector").orderBy(col("close").desc())
df.withColumn("row_num", row_number().over(windowSpec))

4. Feature Engineering

I created key metrics that analysts and traders care about:

Daily Return: (current_close - previous_close) / previous_close
Daily Return %
Volatility: Rolling standard deviation over 7, 14, and 30 days

How it was done:

lag() for previous values
stddev() with window functions for rolling volatility

5. Aggregation by Time Period

Aggregated insights were generated at daily, weekly, and monthly levels. Metrics included average, max, min, and sum.

Implementation:

Dynamically selected numeric columns to aggregate.
Used window functions to keep contextual data while aggregating.
Extracted time features like year(), month(), and weekofyear().

6. Top Movers (Gainers & Losers)

Instead of keeping separate lists for gainers and losers, I combined them into a Top Movers view, showing the best and worst performing stocks by daily return %.

Code snippet:

top_movers = df.orderBy(desc("daily_return_pct")).limit(n)  # Top gainers
top_movers = df.orderBy(asc("daily_return_pct")).limit(n)   # Top losers

7. Gold Layer Storage (Delta Tables)

All cleaned, transformed, and aggregated data is saved as Delta tables, ready for analysis or BI dashboards.

Tables created:

gold_daily_stock_data
gold_weekly_stock_data
gold_monthly_stock_data
gold_top_movers_stock_data
df.write.format("delta").mode("overwrite").saveAsTable("table_name")
8. Key Engineering Concepts
Window Functions for advanced transformations
Schema enforcement for consistent data
Incremental feature engineering
Time-series analysis
Delta Lake for ACID-compliant storage

9. Challenges & How I Solved Them

Issue: Parsing timestamps caused errors (CANNOT_PARSE_TIMESTAMP)
Solution:

Kept dates as strings during processing
Converted to date format only at the final stage using to_date(col("date"), "yyyy-MM-dd")

10. Benefits of This Pipeline

Handles large datasets efficiently
Robust to missing or messy data
Produces ready-to-use analytical tables for BI or ML
Delta Lake ensures reliability and ACID compliance
