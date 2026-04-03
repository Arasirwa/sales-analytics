# 📊 Sales Analytics Pipeline & Power BI Dashboard

## Project Overview

This project demonstrates an end-to-end data analytics pipeline, transforming raw sales data into actionable insights through a structured ETL process and an interactive Power BI dashboard.

The primary goal is to simulate a production-grade analytics workflow: extracting raw data from a source database, cleaning and transforming it with a robust Python pipeline, loading it into a dedicated staging layer, and visualizing the modeled data in Power BI.

---

## Architecture Flow

**Source Database (MySQL)** ➔ **Python ETL Pipeline** ➔ **Staging Database (MySQL)** ➔ **Star Schema Modeling** ➔ **Power BI Dashboard**

---

## Tech Stack

* **Python:** Core pipeline logic (Pandas, NumPy)
* **SQLAlchemy:** Database connection and chunked data loading
* **MySQL:** Relational data storage and staging
* **Power BI:** Data visualization and KPI tracking
* **Dotenv:** Secure environment variable and credential management
* **Logging:** Centralized execution tracking and error handling

---

## The ETL Pipeline

### 1. Extract

* Securely established a database connection using `.env` credentials.
* Extracted raw tables into Pandas DataFrames: `customers`, `markets`, `products`, `transactions`, and `date`.
* Implemented `try/except` blocks to prevent the pipeline from crashing during missing table extractions.

### 2. Transform

* **Data Cleaning:** Filtered out negative revenue records, imputed missing market zones with default values, and standardized text casing across all dimensional codes.
* **Dynamic Currency Conversion:** Vectorized the conversion of USD transactions to INR using a configurable exchange rate stored in the environment variables.
* **Feature Engineering:** Expanded the raw date column into granular time features (Year, Month, Day, Day of Week) to support time-series analysis.

### 3. Load (Staging Layer)

* Preserved data integrity by leaving raw tables untouched and writing exclusively to new `_clean` tables.
* Pushed the transformed DataFrames into a staging database.
* Utilized memory-efficient chunking (`chunksize=1000`) to ensure the pipeline scales gracefully with larger datasets.

---

## Data Modeling

The staging database is structured using a **Star Schema** to optimize query performance for Power BI:

### Fact Table

* `transactions_clean`: Contains quantitative business events (`sales_amount`, `sales_qty`) and foreign keys mapping to the dimensions.

### Dimension Tables

* `customers_clean`: Standardized customer details.
* `products_clean`: Standardized product codes.
* `markets_clean`: Cleaned market zones and names.
* `dates_clean`: Enriched calendar data for time intelligence.

---

## Power BI Dashboard

### Key Metrics (KPIs)

* Total Revenue
* Total Sales Quantity

### Core Visualizations

* Revenue by Market
* Sales Quantity by Market
* Revenue Trend (Time Series)
* Top 5 Customers by Revenue

---

## Setup Instructions

**1. Clone the Repository**

```bash
git clone <your-repo-link>
cd sales-analytics
```

**2. Configure Environment Variables**
Create a `.env` file in the root directory:

```text
DB_USER=your_user
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=3306
DB_NAME=your_raw_db
STAGING_DB_NAME=your_staging_db
USD_TO_INR_RATE=82.0
```

**3. Install Dependencies**

```bash
pip install -r requirements.txt
```

**4. Execute the Pipeline**

```bash
python data_pipeline.py
```

**5. Connect to Power BI**

* Open Power BI Desktop.
* Use the MySQL database connector to load the staging tables.
* Verify the Star Schema relationships in the Model view.

---

## Project Structure

```text
sales-analytics/
├── pipeline.py
├── requirements.txt
├── .env
├── README.md
├── sales_project/
└── powerbi/
```

---

## Key Learnings

* Building a complete, modular ETL pipeline from scratch.
* Securing credentials and business logic using configuration files.
* Writing robust Python code with logging and error handling.
* Designing scalable data models (fact & dimension tables) for BI tools.

---

## Future Improvements

* Automate pipeline scheduling using Airflow or cron jobs.
* Add incremental data loading to process only new transactions.
* Implement data validation checks (e.g., using Great Expectations).
* Deploy the dashboard to the Power BI Service with scheduled refreshes.

---

**Author:** Arasiwa

> ⭐ If you found this repository useful, please give it a star and feel free to connect!
