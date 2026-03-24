# 🛒 Retail Analytics — Spark Medallion Pipeline

A distributed data engineering pipeline built with **Apache Spark** and **Docker**, implementing the **Medallion Architecture** (Bronze → Silver → Gold) on 4 million retail sales records.

---

## 📐 Architecture Overview

```
Raw CSV Data (4M records)
        │
        ▼
┌─────────────────────┐
│    Bronze Layer     │  → Ingest raw CSV with schema enforcement
│  bronze_layer.py    │  → Write as Parquet (24 partitions)
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│    Silver Layer     │  → Deduplicate, clean, validate, standardize
│  silver_layer.py    │  → Write as clean Parquet (8 partitions)
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│     Gold Layer      │  → Business aggregations → 3 data marts
│   gold_layer.py     │  → Daily Sales / Product Performance / City Revenue
└─────────────────────┘
```

---

## 🗂️ Project Structure

```
project/
├── docker-compose.yml          # Spark cluster (1 master + 2 workers)
├── app/
│   ├── generate_raw_data.py    # Generates 4M synthetic retail records
│   ├── bronze_layer.py         # Raw ingestion layer
│   ├── silver_layer.py         # Cleaning and validation layer
│   └── gold_layer.py           # Business aggregation layer
└── data/
    ├── raw/                    # Input: retail_sales_raw.csv
    ├── bronze/                 # Output: retail_sales_bronze.parquet
    ├── silver/                 # Output: retail_sales_clean.parquet
    └── gold/
        ├── daily_sales_metrics.parquet
        ├── product_category_performance.parquet
        └── city_revenue_metrics.parquet
```

---

## 🐳 Spark Cluster Setup

The cluster is defined in `docker-compose.yml` with 3 services:

| Service | Role | Resources | UI |
|---|---|---|---|
| `spark-master` | Cluster Manager | — | `localhost:8088` |
| `spark-worker-1` | Executor | 2 cores, 2GB RAM | `localhost:8081` |
| `spark-worker-2` | Executor | 2 cores, 2GB RAM | `localhost:8082` |

**Total cluster capacity:** 4 cores, 4GB RAM

All three containers share the same `./app` and `./data` volumes, so your Python files and data are accessible inside every container.

---

## ⚙️ Prerequisites

- Docker Desktop installed and running
- Python 3.8+ (for data generation only)
- 10GB+ free disk space (for 4M record dataset)

---

## 🚀 How to Run

### Step 1 — Generate Raw Data

Run this once on your local machine to create the 4M record CSV:

```bash
python app/generate_raw_data.py
```

Output: `data/raw/retail_sales_raw.csv`

---

### Step 2 — Start the Spark Cluster

```bash
docker-compose up -d spark-master spark-worker-1 spark-worker-2
```

Wait ~10 seconds for the cluster to be ready, then verify at `http://localhost:8088`.

---

### Step 3 — Run the Pipeline Layers in Order

**Bronze Layer** — ingest raw CSV:
```bash
docker exec spark-worker-1 \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --conf spark.driver.host=spark-worker-1 \
    --conf spark.driver.bindAddress=0.0.0.0 \
    /opt/spark-app/bronze_layer.py
```

**Silver Layer** — clean and validate:
```bash
docker exec spark-worker-1 \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --conf spark.driver.host=spark-worker-1 \
    --conf spark.driver.bindAddress=0.0.0.0 \
    /opt/spark-app/silver_layer.py
```

**Gold Layer** — build data marts:
```bash
docker exec spark-worker-1 \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --conf spark.driver.host=spark-worker-1 \
    --conf spark.driver.bindAddress=0.0.0.0 \
    /opt/spark-app/gold_layer.py
```

---

### Or Run Everything with One Script

Create `run_pipeline.sh`:

```bash
#!/bin/bash
set -e

echo "Starting Spark Cluster..."
docker-compose up -d spark-master spark-worker-1 spark-worker-2
sleep 10

SUBMIT="docker exec spark-worker-1 /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --conf spark.driver.host=spark-worker-1 \
    --conf spark.driver.bindAddress=0.0.0.0"

echo "Running Bronze Layer..."
$SUBMIT /opt/spark-app/bronze_layer.py
echo "✅ Bronze Done"

echo "Running Silver Layer..."
$SUBMIT /opt/spark-app/silver_layer.py
echo "✅ Silver Done"

echo "Running Gold Layer..."
$SUBMIT /opt/spark-app/gold_layer.py
echo "✅ Gold Done"

echo "Pipeline Complete!"
```

```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
```

---

## 🔄 Layer Details

### Bronze Layer — `bronze_layer.py`

Reads the raw CSV with an **explicit schema** (no inferSchema) and writes it as Parquet.

**Schema enforced:**

| Column | Type |
|---|---|
| `transaction_id` | String |
| `order_date` | Date |
| `ship_date` | Date |
| `customer_id` | String |
| `customer_age` | Integer |
| `gender` | String |
| `product_id` | String |
| `product_category` | String |
| `quantity` | Integer |
| `unit_price` | Float |
| `discount_pct` | Float |
| `city` | String |
| `state` | String |
| `payment_type` | String |
| `order_status` | String |
| `ingestion_date` | Date |

**Output:** `data/bronze/retail_sales_bronze.parquet` (24 partitions)

---

### Silver Layer — `silver_layer.py`

Applies data quality rules to produce a clean, trusted dataset.

| Rule | Logic |
|---|---|
| **Deduplication** | Drop duplicate `transaction_id` rows |
| **Date validation** | Set `ship_date` to null if before `order_date` |
| **Quantity cleaning** | Filter out rows where `quantity <= 0` |
| **Price cleaning** | Set `unit_price` to null if `<= 0` |
| **Discount cleaning** | Set `discount_pct` to null if outside `[0, 100]` |
| **Age cleaning** | Set `customer_age` to null if outside `[15, 100]` |
| **Gender standardization** | Normalize `Male/Female` → `M/F`, null for unknowns |
| **Payment standardization** | Keep only `Card`, `UPI`, `COD` — null for others |

**Output:** `data/silver/retail_sales_clean.parquet` (8 partitions)

---

### Gold Layer — `gold_layer.py`

Builds **3 business-ready data marts** from the clean silver data.

**Derived column:**
```
total_amount = quantity × unit_price × (1 - discount_pct / 100)
```

**Data Mart 1 — Daily Sales Metrics**
```
Group by: order_date
Metrics:  daily_total_revenue, daily_total_orders, daily_avg_order_value
Output:   gold/daily_sales_metrics.parquet
```

**Data Mart 2 — Product Category Performance**
```
Group by: product_category
Metrics:  category_revenue, total_units_sold, order_count
Output:   gold/product_category_performance.parquet
```

**Data Mart 3 — City-Level Revenue**
```
Group by: city, state
Metrics:  city_revenue, order_count, avg_order_value
Output:   gold/city_revenue_metrics.parquet
```

---

## 📊 Dataset Details

Generated by `generate_raw_data.py` — intentionally dirty data for realistic cleaning practice.

| Property | Value |
|---|---|
| Records | 4,000,000 |
| Date range | Jan 2023 – Jan 2026 |
| Cities | 10 US cities |
| Categories | Electronics, Fashion, Grocery, Furniture, Sports |
| Payment types | Card, UPI, COD, Crypto, null |

**Intentional data quality issues introduced:**
- Duplicate `transaction_id` values
- Negative and null `unit_price`
- Invalid `quantity` (zero and negative)
- Out-of-range `discount_pct` (> 100%)
- Unrealistic `customer_age` (negative, > 100)
- Inconsistent `gender` values (`Male`, `Female`, `M`, `F`, null)
- `ship_date` before `order_date`

---

## 🖥️ Monitoring

| URL | What You See |
|---|---|
| `http://localhost:8088` | Spark Master UI — cluster overview, workers, apps |
| `http://localhost:8081` | Worker 1 UI — cores, memory, tasks |
| `http://localhost:8082` | Worker 2 UI — cores, memory, tasks |
| `http://localhost:4045` | Spark Job UI — DAG, stages, task details (while job runs) |

---

## 🛑 Stop the Cluster

```bash
docker-compose down
```

---

## 📊 Power BI Dashboard

The Gold layer Parquet files are connected directly to a **Power BI report** with 4 pages, each visualizing a different business dimension.

### How to Connect Power BI to the Gold Layer

1. Open Power BI Desktop
2. Click **Get Data → Parquet**
3. Load each Gold file:
   - `data/gold/daily_sales_metrics.parquet`
   - `data/gold/product_category_performance.parquet`
   - `data/gold/city_revenue_metrics.parquet`
4. Build relationships between tables on shared keys (`order_date`, `product_category`, `city`)

---

### Page 1 — Product Categories Analytics

Answers: *Which categories drive the most revenue and orders?*

**KPI Cards:**
| Metric | Value |
|---|---|
| Category Revenue | 150.78M |
| Total Orders | 577K |
| Total Sold Units | 3M |

**Visuals:**
- **Pie Chart** — Total Orders per Category: all 5 categories split nearly equally (~20% each), confirming balanced order distribution across Electronics, Fashion, Grocery, Furniture, and Sports
- **Bar Chart** — Revenue by Category: Electronics leads at **62.86M**, followed by Furniture (**46.72M**), Sports (**24.2M**), Fashion (**15.51M**), and Grocery (**1.49M**) — showing Electronics has far higher average unit price despite similar order counts

---

### Page 2 — Units Sold & Revenue (Treemap)

Answers: *How do categories compare on both volume and revenue simultaneously?*

**Visual:** Treemap with tile size = units sold, color = revenue intensity

| Category | Units Sold | Revenue |
|---|---|---|
| Electronics | 633,619 | 62.86M |
| Furniture | 638,498 | 46.72M |
| Sports | 634,413 | 24.20M |
| Fashion | 635,462 | 15.51M |
| Grocery | 630,358 | 1.49M |

**Key insight:** All categories have nearly identical unit volumes (~633K each), but revenue varies dramatically — Electronics generates **42x more revenue** than Grocery from the same number of units, reflecting price differences per category.

---

### Page 3 — Daily Analytics

Answers: *What are the monthly trends in orders and revenue over the year?*

**KPI Cards:**
| Metric | Value |
|---|---|
| Average Daily Revenue | 1.78M |
| Daily Orders | 577K |

**Visuals:**
- **Area Chart — Monthly Orders:** Ranges between 44K–50K per month with a notable dip in February, then recovers. Consistent zigzag pattern suggests alternating high/low months throughout the year
- **Area Chart — Monthly Total Revenue:** Ranges between 11.5M–13.5M per month, peaking in March and May. Revenue trend closely mirrors order volume, with February being the weakest month

---

### Page 4 — States Analytics

Answers: *Which states have the highest average order value?*

**KPI Cards:**
| Metric | Value |
|---|---|
| Total States | 6 |
| Avg Revenue per State | 15.08M |

**Visual:** Choropleth Map (Bing Maps) — US states colored by average order value. Active states: **Texas** (red, highest), **Arizona** (blue), **Illinois** (orange), **Pennsylvania** (green), **New York** (grey), **California** (light grey)

---

### Page 5 — Revenue per City

Answers: *How is revenue geographically distributed across cities?*

**Visual:** Bubble Map — bubble size represents city revenue

| City | State | Notes |
|---|---|---|
| New York | NY | Large bubble — northeast anchor |
| Los Angeles | CA | Large bubble — west coast anchor |
| Chicago | IL | Mid-size bubble — midwest |
| Houston | TX | Mid-size bubble — south |
| Philadelphia | PA | Mid-size bubble — northeast |
| Dallas | TX | Mid-size bubble — south |
| San Antonio | TX | Smaller bubble |
| Phoenix | AZ | Smaller bubble |
| San Diego | CA | Smaller bubble |
| San Jose | CA | Smallest bubble |

**Key insight:** Revenue is spread across all 10 cities with no single dominant city, consistent with the balanced data generation — useful baseline for a real dataset where geographic concentration would emerge.

---

### Dashboard Data Flow

```
Gold Parquet Files
        │
        ├── daily_sales_metrics.parquet       → Page 3 (Daily Analytics)
        ├── product_category_performance.parquet → Page 1 & 2 (Categories + Treemap)
        └── city_revenue_metrics.parquet      → Page 4 & 5 (States + City Map)
                │
                ▼
        Power BI Desktop
                │
                ▼
        Published to Power BI Service (optional)
```

## **Connect a BI tool (Power BI) to the Gold layer outputs**

- The Power BI file [Power BI File](./Retail_Analytics_Dashboard.pbix)

    - Product Categories Analytics
        ![Product Categories Analytics](/Pictures/product_category.png)

    - Units Sold & Revenue
        ![Units Sold & Revenue](/Pictures/Units_Sold&Revenue.png)

    - Daily Analytics
        ![Daily Analytics](/Pictures/Daily_Analytics.png)

    - States Analytics
        ![States Analytics](/Pictures/States_Analytics.png)

    -  Revenue per City
        ![ Revenue per City](/Pictures/Revenue_per_City.png)                

---

## 🔮 Next Steps

- **Add Apache Airflow** to schedule and orchestrate the pipeline automatically
- **Migrate to Databricks** with Delta Lake instead of plain Parquet
- **Add data quality checks** using Great Expectations or Delta constraints
- **Deploy to AWS** using S3 + EMR for production scale
