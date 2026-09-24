"""
===============================================================================
PIPELINE STAGE: Silver Layer (Data Cleansing, Transformation & Quarantine)
===============================================================================

Overview:
---------
This module forms the transformation engine of the Medallion Architecture pipeline.
It extracts raw transactional data from `bronze_data`, enforces strict data quality
rules, isolates corrupted primary keys into an audit quarantine table, and loads
cleansed records into `silver_data`.

Architecture Highlights:
------------------------
1. Streaming / Batch Ingestion:
   - Implements chunked processing (50,000 rows per batch) using database cursors
     (`fetchmany`) to process 1,000,000 records under constant O(1) memory footprint.

2. Primary Key Collision Auditing (Zero Data Loss):
   - Rather than silently dropping or aborting on duplicate records, duplicated
     `transaction_id` rows are intercepted and archived into `silver_quarantine`
     with clear audit flags.

3. Field-Level Standardization:
   - Primary and Foreign Keys: Cast to explicit integers with whitespace trimmed.
   - Text Formatting: Capitalization and Title casing applied across names,
     cities, and product categories.
   - Missing Value Imputation: NULL string literals and blanks normalized to 'n/a'.
   - Anomaly Rectification: Negative quantities and prices corrected to positive
     via absolute values; zero quantities mapped to database NULLs.
   - Date Standard: Re-formats non-ISO dates (DD/MM/YYYY) to ISO-8601 (YYYY-MM-DD).
   - Audit Lineage: Captures high-precision processing timestamps per record.

4. Production Logging:
   - Implements dual-stream logging (Console + `silver_layer.log`).
===============================================================================
"""

from datetime import datetime
import logging
import os
import sqlite3

# =============================================================================
# Logging Configuration
# =============================================================================
LOG_FILE = "silver_layer.log"
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
def silver_database_connection(db_name="Database.db"):
    """
    Establishes and verifies connection to the SQLite database.
    
    Args:
        db_name (str): Path or filename of the target database.
        
    Returns:
        sqlite3.Connection: Database connection object.
    """
    database_file = sqlite3.connect(db_name)
    full_path = os.path.abspath(db_name)
    logging.info(" >>> Connected to Database File <<< ")
    logging.info(f" >>> File Location: {full_path} <<< ")
    logging.info("-" * 70)
    return database_file


# =============================================================================
# Step 2: DDL Schemas Initialization
# =============================================================================
def silver_table_creation(database_file):
    """
    Initializes DDL schemas for the Silver Layer:
      1. `silver_data`: Enforces typed schemas and primary key uniqueness.
      2. `silver_quarantine`: Retains rejected collision records for auditability.
    
    Args:
        database_file (sqlite3.Connection): Active database connection.
        
    Returns:
        sqlite3.Connection: Active database connection.
    """
    cursor = database_file.cursor()

    # Drop existing tables to guarantee an idempotent pipeline run
    cursor.execute("DROP TABLE IF EXISTS silver_data;")
    cursor.execute("""
        CREATE TABLE silver_data (
            transaction_id      INTEGER PRIMARY KEY,
            customer_id         INTEGER,
            customer_name       TEXT,
            customer_email      TEXT,
            store_city          TEXT,
            product_category    TEXT,
            quantity            INTEGER,
            unit_price          REAL,
            transaction_date    DATE,
            ingestion_timestamp TEXT
        );
    """)

    cursor.execute("DROP TABLE IF EXISTS silver_quarantine;")
    cursor.execute("""
        CREATE TABLE silver_quarantine (
            quarantine_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            source_transaction_id TEXT,
            customer_id           TEXT,
            customer_name         TEXT,
            customer_email        TEXT,
            store_city            TEXT,
            product_category      TEXT,
            quantity              TEXT,
            unit_price            TEXT,
            transaction_date      TEXT,
            rejection_reason      TEXT,
            quarantine_timestamp  DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    """)
    database_file.commit()
    logging.info(" >>> Silver and Quarantine Tables Created Successfully <<< ")
    logging.info("-" * 70)
    return database_file


# =============================================================================
# Step 3: Primary Key Collision Isolation (Quarantine)
# =============================================================================
def load_silver_quarantine(database_file):
    """
    Identifies primary key collisions in bronze_data using a CTE,
    and isolates all duplicate records into silver_quarantine.
    
    Args:
        database_file (sqlite3.Connection): Active database connection.
    """
    cursor = database_file.cursor()
    
    quarantine_query = """
    WITH collided_keys AS (
        SELECT 
            TRIM(transaction_id) AS clean_tx_id
        FROM bronze_data
        GROUP BY TRIM(transaction_id)
        HAVING COUNT(*) > 1
    )
    INSERT INTO silver_quarantine (
        source_transaction_id,
        customer_id,
        customer_name,
        customer_email,
        store_city,
        product_category,
        quantity,
        unit_price,
        transaction_date,
        rejection_reason
    )
    SELECT 
        b.transaction_id,
        b.customer_id,
        b.customer_name,
        b.customer_email,
        b.store_city,
        b.product_category,
        b.quantity,
        b.unit_price,
        b.transaction_date,
        'PRIMARY_KEY_COLLISION'
    FROM bronze_data b
    JOIN collided_keys c 
      ON TRIM(b.transaction_id) = c.clean_tx_id;
    """
    cursor.execute(quarantine_query)
    database_file.commit()
    
    # Query total records quarantined for verification
    cursor.execute("SELECT COUNT(*) FROM silver_quarantine WHERE rejection_reason = 'PRIMARY_KEY_COLLISION';")
    total_quarantined = cursor.fetchone()[0]
    logging.info(f" >>> Quarantine Population Completed: {total_quarantined:,} records isolated <<< ")
    logging.info("-" * 70)


# =============================================================================
# Step 4: Granular Row Transformation Engine
# =============================================================================
def clean_data(data_row):
    """
    Applies granular cleaning, type casting, and standardizations to an individual row.
    
    Args:
        data_row (tuple): A raw tuple row extracted from bronze_data.
        
    Returns:
        tuple: Cleansed and standardized record matching silver_data schema.
    """
    (
        transaction_id, customer_id, customer_name, customer_email,
        store_city, product_category, quantity, unit_price,
        transaction_date, ingestion_timestamp
    ) = data_row

    # Numerical identifiers casting
    clean_trans_id = int(str(transaction_id).strip())
    clean_cs_id = int(str(customer_id).strip())

    # Text normalization
    clean_cs_nm = str(customer_name).strip().title() if customer_name else None

    # Handle string literal NULLs and empty strings
    clean_cs_email = "n/a"
    if customer_email:
        val = str(customer_email).strip()
        if val.upper() != "NULL" and val != "":
            clean_cs_email = val.lower()

    clean_store_city = str(store_city).strip().capitalize()
    clean_prd_cat = str(product_category).strip().title()

    # Numerical anomaly resolution: Rectify negatives, map zero to None
    clean_quantity = int(str(quantity).strip())
    if clean_quantity < 0:
        clean_quantity = abs(clean_quantity)
    elif clean_quantity == 0:
        clean_quantity = None

    clean_price = float(str(unit_price).strip())
    if clean_price < 0:
        clean_price = abs(clean_price)
    elif clean_price == 0:
        clean_price = None

    # Date normalization: Standardize DD/MM/YYYY into ISO-8601 (YYYY-MM-DD)
    clean_date = str(transaction_date).strip()
    if "/" in clean_date:
        date_parts = clean_date.split("/")
        clean_date = f"{date_parts[2]}-{date_parts[1]}-{date_parts[0]}"

    # Audit lineage: Stamp processing time for Silver layer
    silver_time_ingestion = datetime.now().isoformat()

    return (
        clean_trans_id, clean_cs_id, clean_cs_nm, clean_cs_email,
        clean_store_city, clean_prd_cat, clean_quantity, clean_price,
        clean_date, silver_time_ingestion
    )


# =============================================================================
# Step 5: Streamed Batch Processing & Ingestion
# =============================================================================
def load_silver_data(database_file, chunk_size=50000):
    """
    Streams bronze_data in configurable chunks, filters out quarantined records,
    transforms valid records, and batch-inserts them into silver_data.
    
    Args:
        database_file (sqlite3.Connection): Active database connection.
        chunk_size (int): Batch size per iteration for memory efficiency.
    """
    bronze_cursor = database_file.cursor()
    silver_cursor = database_file.cursor()

    # Preload quarantined IDs into an in-memory hash set for O(1) lookup speed
    bronze_cursor.execute("SELECT source_transaction_id FROM silver_quarantine;")
    quarantined_ids = {str(row[0]).strip() for row in bronze_cursor.fetchall()}

    # Stream query cursor initialization
    select_query = """
        SELECT 
            transaction_id, 
            customer_id, 
            customer_name, 
            customer_email, 
            store_city, 
            product_category, 
            quantity, 
            unit_price, 
            transaction_date, 
            ingestion_timestamp 
        FROM bronze_data;
    """
    bronze_cursor.execute(select_query)

    insert_query = """
        INSERT INTO silver_data (
            transaction_id, customer_id, customer_name, customer_email,
            store_city, product_category, quantity, unit_price,
            transaction_date, ingestion_timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    total_inserted = 0
    batch_num = 1

    # Chunked extraction and transformation loop
    while True:
        batch = bronze_cursor.fetchmany(chunk_size)
        if not batch:
            break

        cleaned_batch = []
        for row in batch:
            trans_id = str(row[0]).strip()
            # Skip records flagged for quarantine
            if trans_id in quarantined_ids:
                continue
            cleaned_row = clean_data(row)
            cleaned_batch.append(cleaned_row)

        # Batch insert clean records and commit transaction
        if cleaned_batch:
            silver_cursor.executemany(insert_query, cleaned_batch)
            database_file.commit()

        total_inserted += len(cleaned_batch)
        logging.info(f" >>> Batch {batch_num:02d} processed: {len(cleaned_batch):,} rows inserted (Total so far: {total_inserted:,}) <<<")
        batch_num += 1

    logging.info("-" * 70)
    logging.info(f" >>> Total Silver Records Inserted: {total_inserted:,} <<< ")
    logging.info("-" * 70)


# =============================================================================
# Pipeline Execution Flow
# =============================================================================
if __name__ == "__main__":
    db_conn = silver_database_connection()
    try:
        silver_table_creation(db_conn)
        load_silver_quarantine(db_conn)
        load_silver_data(db_conn)
    finally:
        db_conn.close()
        logging.info(" >>> Database Connection Safely Closed <<< ")