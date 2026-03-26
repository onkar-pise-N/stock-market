

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
          ┌────────────┴────────────┐
          ▼                         ▼
┌─────────────────────┐   ┌─────────────────────┐
│ Top Gainers         │   │ Top Losers          │
│ (High Return %)     │   │ (Low Return %)      │
└────────────┬────────┘   └────────────┬────────┘
             │                         │
             └────────────┬────────────┘
                          ▼
        ┌──────────────────────────────┐
        │   Gold Layer (Delta Tables)  │
        │ - Daily Table                │
        │ - Weekly Table               │
        │ - Monthly Table              │
        │ - Gainers Table              │
        │ - Losers Table               │
        └──────────────┬───────────────┘
                       │
                       ▼
        ┌──────────────────────────────┐
        │                              │
        │   DataBricks DashBoard       │
        └──────────────────────────────┘
------------------------------------------------------------------------------------------------------------------


📊 Stock Market Data Engineering Pipeline (PySpark + Delta Lake)

This project is a scalable data engineering pipeline built using PySpark to process stock market data from raw ingestion to curated analytical datasets (Gold Layer). The pipeline performs data cleaning, transformation, feature engineering, aggregation, and storage using Delta Lake.

🔹 1. Data Ingestion (Schema-Based Reading)
Data is ingested from Azure Blob Storage in CSV format.
A custom schema is defined instead of schema inference to ensure:
Data consistency
Better performance
Avoidance of incorrect data types
🧩 Code Approach:
Used StructType and StructField to define schema.
Read data using:
spark.read.csv(..., header=True, schema=schema)
🔹 2. Data Validation & Column Standardization
Ensured all required columns exist.
Handled missing columns dynamically using lit(None).
Sanitized column names (lowercase, removed special characters) for SQL compatibility.
🧩 Code Logic:
Loop through expected columns and add missing ones.
Rename columns using:
df.withColumnRenamed(...)
🔹 3. Data Cleaning & Deduplication
Removed duplicate records using window functions.
Deduplication logic:
Partition by date and sector
Keep the latest/highest close value
🧩 Code Approach:
Used Window.partitionBy() and row_number():
windowSpec = Window.partitionBy("date", "sector").orderBy(col("close").desc())
Forward-filled missing values using:
last(column, True).over(windowSpec)
🔹 4. Feature Engineering

Added multiple derived features for analysis:

📈 Daily Return
Formula:
(current_close - previous_close) / previous_close
📊 Daily Return Percentage
daily_return * 100
📉 Volatility
Rolling standard deviation over:
7 days
14 days
30 days
🧩 Code Techniques:
Used lag() for previous values
Used stddev() with window functions:
Window.partitionBy("sector").orderBy("date").rowsBetween(...)
🔹 5. Time-Based Aggregation

Generated aggregated insights at multiple levels:

Daily
Weekly
Monthly
📊 Metrics Calculated:
Average (avg)
Maximum (max)
Minimum (min)
Sum (sum)
🧩 Code Approach:
Dynamically selected numeric columns:
[c for c, t in df.dtypes if t in ["double"]]
Used window functions instead of groupBy to retain original columns:
avg(col(metric)).over(window_spec)
Extracted time features:
year(), month(), weekofyear()
🔹 6. Top Gainers & Losers Identification
Identified best and worst performing sectors/stocks.
📈 Logic:
Top Gainers → highest daily_return_pct
Top Losers → lowest daily_return_pct
🧩 Code:
df.orderBy(desc("daily_return_pct")).limit(n)
df.orderBy(asc("daily_return_pct")).limit(n)
🔹 7. Data Storage (Gold Layer - Delta Tables)
Final processed data is stored as Delta Tables using saveAsTable().
📦 Tables Created:
gold_daily_stock_data
gold_weekly_stock_data
gold_monthly_stock_data
gold_top_gainers_stock_data
gold_top_losers_stock_data
🧩 Code:
df.write.format("delta").mode("overwrite").saveAsTable("table_name")
🔹 8. Key Engineering Concepts Used
Window Functions (advanced transformations)
Schema Enforcement
Incremental Feature Engineering
Data Cleaning Strategies
Time-Series Analysis
Delta Lake Storage
🔹 9. Challenges & Solutions
❌ Issue:
Date parsing errors (CANNOT_PARSE_TIMESTAMP)
✅ Solution:
Avoided early parsing, used string sorting
Converted to date only at final stage using:
to_date(col("date"), "yyyy-MM-dd")
🔹 10. Benefits of This Pipeline
Scalable for large datasets
Robust against dirty/missing data
Efficient due to window-based processing
Ready for BI tools or ML models
Delta format ensures ACID compliance
✅ Conclusion

This project demonstrates how to build a production-ready data pipeline using PySpark, handling real-world challenges like schema evolution, missing data, and time-series analytics, while delivering structured insights for business use.
