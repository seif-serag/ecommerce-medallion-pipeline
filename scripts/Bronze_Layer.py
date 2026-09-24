"""
===============================================================================
PIPELINE STAGE: Bronze Layer Ingestion (Raw Landing Layer)
===============================================================================

Overview:
---------
This module implements the Bronze (Raw Landing) ingestion phase of a Medallion
Architecture data engineering pipeline using vanilla Python and SQLite.

Key Highlights:
---------------
1. High-Throughput Stream Ingestion:
   - Ingests 1,000,000 raw transactional records from CSV into SQLite using a 
     line-by-line generator/stream without loading the entire dataset into memory.

2. Low Memory Footprint:
   - Employs batching (50,000 rows/batch) via `executemany` to ensure minimal
     RAM allocation during ingestion.

3. Database Engine Optimization:
   - Employs SQLite PRAGMA directives (`PRAGMA synchronous = OFF` and 
     `PRAGMA journal_mode = MEMORY`) to bypass disk synchronization overhead
     for maximum bulk insertion speed.

4. Fault-Tolerant Raw Schema:
   - Defines all columns as untyped `TEXT` fields. This absorbs anomalies, nulls,
     and malformed formats safely without raising runtime schema casting errors.

5. Production Logging & Idempotency:
   - Implements structured dual-stream logging (Console + `bronze_ingestion.log`).
   - Ensures pipeline idempotency by dropping and recreating tables prior to ingestion.
===============================================================================
"""

import csv
import logging
import os
import sqlite3
import time

# =============================================================================
# Logging Configuration
# =============================================================================
LOG_FILE = "bronze_ingestion.log"
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


# =============================================================================
# Step 1: Database Connection Setup
# =============================================================================
def bronze_database_file_creation(db_name: str = "Database.db") -> sqlite3.Connection:
    """
    Initializes and establishes a connection to the target SQLite database.

    Args:
        db_name (str): The filename or path of the target SQLite database.

    Returns:
        sqlite3.Connection: Active database connection handle.
    """
    bronze_db_file = sqlite3.connect(db_name)
    full_path = os.path.abspath(db_name)

    logging.info(" >>> Bronze Layer Database File Is Connected <<< ")
    logging.info(f" >>> File Location: {full_path} <<< ")
    logging.info("-" * 70)
    return bronze_db_file


# =============================================================================
# Step 2: DDL Landing Table Creation
# =============================================================================
def bronze_table_creation(bronze_db_file: sqlite3.Connection) -> sqlite3.Connection:
    """
    Recreates the bronze landing table (bronze_data).

    Drops existing tables to maintain pipeline idempotency. All fields are
    defined as TEXT to absorb raw, unvalidated strings, nulls, and malformed
    records without triggering type casting errors during ingestion.

    Args:
        bronze_db_file (sqlite3.Connection): Active database connection.

    Returns:
        sqlite3.Connection: Database connection reference.
    """
    cursor = bronze_db_file.cursor()

    # Drop existing table to ensure an idempotent, clean run
    cursor.execute("DROP TABLE IF EXISTS bronze_data;")

    # Define the Bronze landing table schema
    cursor.execute("""
    CREATE TABLE bronze_data (
        transaction_id      TEXT,
        customer_id         TEXT,
        customer_name       TEXT,
        customer_email      TEXT,
        store_city          TEXT,
        product_category    TEXT,
        quantity            TEXT,
        unit_price          TEXT,
        transaction_date    TEXT,
        ingestion_timestamp TEXT
    );
    """)
    bronze_db_file.commit()

    logging.info(" >>> Bronze Table (bronze_data) Schema Initialized <<< ")
    logging.info("-" * 70)
    return bronze_db_file


# =============================================================================
# Step 3: Streamed Batch Ingestion
# =============================================================================
def bronze_data_insert(
    bronze_db_file: sqlite3.Connection,
    csv_path: str = r"E:\Data Engineering\python\( Python + SQL ) Project\bronze_transactions_raw.csv",
    batch_size: int = 50000,
) -> None:
    """
    Streams raw records from a CSV file directly into SQLite using chunked batch inserts.

    Args:
        bronze_db_file (sqlite3.Connection): Active database connection.
        csv_path (str): File path to the raw source CSV.
        batch_size (int): Number of rows accumulated per bulk insert batch.
    """
    cursor = bronze_db_file.cursor()

    # Engine tuning: bypass disk sync and journal overhead for maximum bulk write speed
    cursor.execute("PRAGMA synchronous = OFF;")
    cursor.execute("PRAGMA journal_mode = MEMORY;")

    insert_query = """
    INSERT INTO bronze_data (
        transaction_id, customer_id, customer_name, customer_email,
        store_city, product_category, quantity, unit_price,
        transaction_date, ingestion_timestamp
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    batch = []
    total_inserted = 0

    logging.info(" >>> Starting Streaming Data Ingestion into Bronze Table <<< ")
    logging.info(f" >>> Source CSV: {os.path.abspath(csv_path)} <<< ")
    start_time = time.time()

    # Stream the CSV line-by-line to prevent high memory consumption
    with open(csv_path, mode="r", encoding="utf-8") as file:
        reader = csv.reader(file)
        next(reader)  # Skip the CSV header row

        for row in reader:
            batch.append(row)

            # Flush batch to database when threshold is reached
            if len(batch) == batch_size:
                cursor.executemany(insert_query, batch)
                bronze_db_file.commit()
                total_inserted += len(batch)
                logging.info(f" -> Inserted: {total_inserted:,} rows...")
                batch.clear()

        # Flush any remaining records smaller than the batch size
        if batch:
            cursor.executemany(insert_query, batch)
            bronze_db_file.commit()
            total_inserted += len(batch)
            logging.info(f" -> Inserted: {total_inserted:,} rows (Final batch)...")
            batch.clear()

    elapsed = time.time() - start_time
    logging.info("-" * 70)
    logging.info(
        f" >>> Done! Successfully inserted {total_inserted:,} rows in {elapsed:.2f}s <<< "
    )
    logging.info("-" * 70)


# =============================================================================
# Execution Entry Point
# =============================================================================
if __name__ == "__main__":
    # 1. Initialize and connect to the database
    db_conn = bronze_database_file_creation()

    try:
        # 2. Recreate the Bronze landing table
        bronze_table_creation(db_conn)

        # 3. Stream and ingest CSV data in batches
        bronze_data_insert(db_conn)
    finally:
        # 4. Safely close database connection
        db_conn.close()
        logging.info(" >>> Database Connection Safely Closed <<< ")