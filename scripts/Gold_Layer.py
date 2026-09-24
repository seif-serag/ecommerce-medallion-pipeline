"""
===============================================================================
PIPELINE STAGE: Gold Layer (Curated Business Layer & Star Schema)
===============================================================================

Overview:
---------
This module implements the Gold (Curated / Consumption) Layer using an 
Object-Oriented Programming (OOP) pattern. It decouples transformation logic 
from database compute:
  - In-Memory Processing: Business transformations, entity resolution, date
    dimensional parsing, and KPI aggregations are computed directly in Python
    using high-performance data structures (Hash Maps / Dictionaries).
  - Streamed Execution: High-volume transaction tables are processed in chunks 
    (50,000 rows/batch) to maintain an O(1) memory footprint.
  - Storage I/O: SQLite is utilized purely as an ingestion target and persistence
    layer, ensuring zero compute bottlenecks on the relational engine.

Data Models Built:
------------------
1. Fact Table:
   - `fact_sales`: Transactional facts with calculated metrics (`total_sales_amount`).

2. Dimension Tables:
   - `dim_customers`: Deduplicated golden records for unique customers, resolving 
     missing emails and names across customer interactions.
   - `dim_dates`: Calendar dimension decomposing ISO transaction dates into Year,
     Month, Day, Quarter, and Day of Week for fast BI time intelligence.

3. Business Data Mart:
   - `mart_city_performance`: Pre-aggregated executive KPIs per store city
     (Total Orders, Units Sold, Total Revenue, Average Order Value).

4. Production Logging:
   - Dual-stream logging configured via standard library (`gold_layer.log` + Console).
===============================================================================
"""

from collections import defaultdict
from datetime import datetime
import logging
import os
import sqlite3

# =============================================================================
# Logging Configuration
# =============================================================================
LOG_FILE = "gold_layer.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# =============================================================================
# Working Directory Setup
# =============================================================================
PROJECT_DIR = r"E:\Data Engineering\python\( Python + SQL ) Project"
if os.path.exists(PROJECT_DIR):
    os.chdir(PROJECT_DIR)
    logging.info(f"Working directory set to: {PROJECT_DIR}")
else:
    logging.warning(f"Project directory not found: {PROJECT_DIR}. Using current runtime directory.")


class GoldPipeline:
    """
    Orchestrates the creation and population of the Gold Layer tables
    utilizing in-memory Python transformations and dimensional modeling.
    """

    def __init__(self, db_path="Database.db"):
        """
        Initializes the database connection and operational cursors.

        Args:
            db_path (str): Path to SQLite database file.
        """
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        logging.info(" >>> Connected to Database File via GoldPipeline <<< ")
        logging.info(f" >>> Database File: {os.path.abspath(self.db_path)} <<< ")
        logging.info("-" * 70)

    def create_gold_tables(self):
        """
        Initializes DDL schemas for all Gold layer target tables:
          - `fact_sales`
          - `dim_customers`
          - `dim_dates`
          - `mart_city_performance`
        """
        # 1. Fact Table: Sales
        self.cursor.execute("DROP TABLE IF EXISTS fact_sales;")
        self.cursor.execute("""
            CREATE TABLE fact_sales (
                transaction_id      INTEGER PRIMARY KEY,
                customer_id         INTEGER,
                store_city          TEXT,
                product_category    TEXT,
                transaction_date    DATE,
                quantity            INTEGER,
                unit_price          REAL,
                total_sales_amount  REAL,
                gold_timestamp      TEXT
            );
        """)

        # 2. Dimension Table: Customers
        self.cursor.execute("DROP TABLE IF EXISTS dim_customers;")
        self.cursor.execute("""
            CREATE TABLE dim_customers (
                customer_id     INTEGER PRIMARY KEY,
                customer_name   TEXT,
                customer_email  TEXT,
                gold_timestamp  TEXT
            );
        """)

        # 3. Dimension Table: Calendar Dates
        self.cursor.execute("DROP TABLE IF EXISTS dim_dates;")
        self.cursor.execute("""
            CREATE TABLE dim_dates (
                date_id         DATE PRIMARY KEY,
                year            INTEGER,
                month           INTEGER,
                day             INTEGER,
                quarter         TEXT,
                day_of_week     TEXT,
                gold_timestamp  TEXT
            );
        """)

        # 4. Data Mart: City Performance
        self.cursor.execute("DROP TABLE IF EXISTS mart_city_performance;")
        self.cursor.execute("""
            CREATE TABLE mart_city_performance (
                store_city          TEXT PRIMARY KEY,
                total_transaction   INTEGER,
                total_units_sold    INTEGER,
                total_revenu        REAL,
                avr_order_value     REAL,
                gold_timestamp      TEXT
            );
        """)

        self.conn.commit()
        logging.info(" >>> Gold Layer Tables Created Successfully <<< ")
        logging.info("-" * 70)

    def build_fact_sales(self, chunk_size=50000):
        """
        Streams clean records from `silver_data` in chunks, computes
        `total_sales_amount` in Python, stamps ingestion time, and batch inserts
        into `fact_sales`.

        Args:
            chunk_size (int): Number of records per streaming batch.
        """
        logging.info(" >>> Building fact_sales via Python streaming... <<< ")
        read_cursor = self.conn.cursor()
        write_cursor = self.conn.cursor()

        select_query = """
            SELECT 
                transaction_id, 
                customer_id, 
                store_city, 
                product_category, 
                transaction_date, 
                quantity, 
                unit_price 
            FROM silver_data;
        """
        read_cursor.execute(select_query)

        insert_query = """
            INSERT INTO fact_sales (
                transaction_id, customer_id, store_city, product_category,
                transaction_date, quantity, unit_price, total_sales_amount,
                gold_timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        total_inserted = 0
        batch_num = 1

        while True:
            batch = read_cursor.fetchmany(chunk_size)
            if not batch:
                break

            processed_batch = []
            for row in batch:
                tx_id, cust_id, city, cat, tx_date, qty, price = row

                # Calculate total revenue per transaction in-memory
                if qty is not None and price is not None:
                    total_amount = round(qty * price, 2)
                else:
                    total_amount = None

                gold_ts = datetime.now().isoformat()

                processed_batch.append((
                    tx_id, cust_id, city, cat, tx_date, qty, price, total_amount, gold_ts
                ))

            write_cursor.executemany(insert_query, processed_batch)
            self.conn.commit()

            total_inserted += len(processed_batch)
            logging.info(f" >>> fact_sales: Batch {batch_num:02d} processed ({total_inserted:,} rows inserted) <<<")
            batch_num += 1

        logging.info("-" * 70)
        logging.info(f" >>> fact_sales Completed: {total_inserted:,} records loaded <<< ")
        logging.info("-" * 70)

    def build_dim_customers(self):
        """
        Builds a single source of truth (Golden Record) for each customer.
        Performs in-memory entity resolution using nested dictionaries to ensure
        valid emails and non-null names are preserved over missing/placeholder values.
        """
        logging.info(" >>> Processing dim_customers in Python Memory... <<< ")
        read_cursor = self.conn.cursor()
        write_cursor = self.conn.cursor()

        read_cursor.execute("SELECT customer_id, customer_name, customer_email FROM silver_data;")

        # Nested dictionary for O(1) in-memory lookups and updates
        customers_map = {}

        for row in read_cursor.fetchall():
            cust_id, name, email = row

            if cust_id not in customers_map:
                customers_map[cust_id] = {
                    'name': name,
                    'email': email
                }
            else:
                # Update missing email ('n/a') if a valid email is encountered
                if customers_map[cust_id]['email'] == 'n/a' and email != 'n/a':
                    customers_map[cust_id]['email'] = email
                # Impute missing name if a valid name is encountered
                if not customers_map[cust_id]['name'] and name:
                    customers_map[cust_id]['name'] = name

        gold_ts = datetime.now().isoformat()
        records_to_insert = [
            (cust_id, data['name'], data['email'], gold_ts)
            for cust_id, data in customers_map.items()
        ]

        insert_query = """
            INSERT INTO dim_customers (customer_id, customer_name, customer_email, gold_timestamp)
            VALUES (?, ?, ?, ?);
        """
        write_cursor.executemany(insert_query, records_to_insert)
        self.conn.commit()

        logging.info(f" >>> dim_customers Completed: {len(records_to_insert):,} unique customers loaded <<< ")
        logging.info("-" * 70)

    def build_dim_dates(self):
        """
        Extracts unique ISO transaction dates from `silver_data`, parses
        them using Python's datetime module into analytical components
        (Year, Month, Day, Quarter, Day of Week), and populates `dim_dates`.
        """
        logging.info(" >>> Processing dim_dates in Python Memory... <<< ")
        read_cursor = self.conn.cursor()
        write_cursor = self.conn.cursor()

        read_cursor.execute("SELECT DISTINCT transaction_date FROM silver_data WHERE transaction_date IS NOT NULL;")
        unique_dates = [row[0] for row in read_cursor.fetchall()]

        gold_ts = datetime.now().isoformat()
        dates_to_insert = []

        for date_str in sorted(unique_dates):
            # Parse ISO date string (YYYY-MM-DD)
            dt = datetime.strptime(date_str, "%Y-%m-%d")

            year = dt.year
            month = dt.month
            day = dt.day
            quarter = f"Q{(month - 1) // 3 + 1}"
            day_of_week = dt.strftime("%A")

            dates_to_insert.append((
                date_str, year, month, day, quarter, day_of_week, gold_ts
            ))

        insert_query = """
            INSERT INTO dim_dates (date_id, year, month, day, quarter, day_of_week, gold_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?);
        """
        write_cursor.executemany(insert_query, dates_to_insert)
        self.conn.commit()

        logging.info(f" >>> dim_dates Completed: {len(dates_to_insert):,} dates loaded <<< ")
        logging.info("-" * 70)

    def build_mart_city_performance(self):
        """
        Computes business-level aggregations in-memory across all cities using Python.
        Aggregates Total Transactions, Units Sold, and Revenue, calculates Average Order Value,
        and saves summary KPIs into `mart_city_performance`.
        """
        logging.info(" >>> Aggregating mart_city_performance in Python Memory... <<< ")
        read_cursor = self.conn.cursor()
        write_cursor = self.conn.cursor()

        # Extract only columns needed for aggregation
        read_cursor.execute("SELECT store_city, quantity, total_sales_amount FROM fact_sales;")

        # In-memory accumulator structure
        city_aggregations = defaultdict(lambda: {
            "total_transactions": 0,
            "total_units_sold": 0,
            "total_revenue": 0.0
        })

        for row in read_cursor.fetchall():
            city, qty, amount = row
            if not city:
                continue

            city_aggregations[city]["total_transactions"] += 1
            if qty is not None:
                city_aggregations[city]["total_units_sold"] += qty
            if amount is not None:
                city_aggregations[city]["total_revenue"] += amount

        gold_ts = datetime.now().isoformat()
        mart_records = []

        for city, stats in sorted(city_aggregations.items()):
            tx_count = stats["total_transactions"]
            units_sold = stats["total_units_sold"]
            total_rev = round(stats["total_revenue"], 2)
            avg_order_val = round(total_rev / tx_count, 2) if tx_count > 0 else 0.0

            mart_records.append((
                city, tx_count, units_sold, total_rev, avg_order_val, gold_ts
            ))

        insert_query = """
            INSERT INTO mart_city_performance (
                store_city, total_transaction, total_units_sold,
                total_revenu, avr_order_value, gold_timestamp
            ) VALUES (?, ?, ?, ?, ?, ?);
        """
        write_cursor.executemany(insert_query, mart_records)
        self.conn.commit()

        logging.info(f" >>> mart_city_performance Completed: {len(mart_records):,} cities summarized <<< ")
        logging.info("-" * 70)

    def run_pipeline(self):
        """
        Sequentially executes all Gold Layer ingestion and transformation tasks.
        """
        logging.info("=" * 70)
        logging.info(" >>> STARTING GOLD LAYER PIPELINE EXECUTION <<< ")
        logging.info("=" * 70)

        self.create_gold_tables()
        self.build_fact_sales()
        self.build_dim_customers()
        self.build_dim_dates()
        self.build_mart_city_performance()

        logging.info("=" * 70)
        logging.info(" >>> GOLD LAYER PIPELINE COMPLETED SUCCESSFULLY <<< ")
        logging.info("=" * 70)

    def close(self):
        """Closes the underlying SQLite database connection."""
        self.conn.close()
        logging.info(" >>> Database Connection Closed <<< ")


# =============================================================================
# Pipeline Execution Entry Point
# =============================================================================
if __name__ == "__main__":
    pipeline = GoldPipeline(db_path="Database.db")
    try:
        pipeline.run_pipeline()
    finally:
        pipeline.close()