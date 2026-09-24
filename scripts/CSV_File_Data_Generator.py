"""
===============================================================================
PIPELINE STAGE: Synthetic Data Generation (Source Simulator)
===============================================================================

Overview:
---------
This module simulates a high-throughput, real-world transactional e-commerce engine.
It generates a large-scale synthetic dataset (1,000,000 records) formatted as CSV
to serve as the upstream source for the Medallion Data Pipeline.

Key Engineering Features:
-------------------------
1. Master Customer Data Pool:
   - Maintains a consistent relational pool of 30,000 realistic customer identities.
   - Preserves referential integrity between customer attributes across transactions.

2. Realistic Noise & Anomaly Injection:
   - Human data entry anomalies (whitespace padding, casing inconsistencies).
   - Missing/dirty emails (~3% empty strings, literal 'NULL', or padded tokens).
   - System error artifacts (zero/negative transaction quantities and unit prices).
   - Mixed date formats (ISO YYYY-MM-DD vs DD/MM/YYYY).
   - Primary Key collision / Duplicate transactions (~0.1% frequency).

3. Memory-Efficient Streaming I/O:
   - Chunked batch processing (50,000 records/batch) using Python's standard `csv.writer`
     to generate large volumes without exhausting system memory.

4. Production Logging:
   - Implements Python standard `logging` configured for dual output (File + Console).
===============================================================================
"""

import csv
from datetime import datetime, timedelta
import logging
import os
import random
import time

# =============================================================================
# Logging Configuration
# =============================================================================
LOG_FILE = "data_generation.log"
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
    logging.warning(f"Project directory not found: {PROJECT_DIR}. Using default current directory.")

# =============================================================================
# Reference Data Constants
# =============================================================================
FIRST_NAMES = [
    "Ahmed", "Mohamed", "Sara", "Omar", "Youssef", "Nour", "Khaled", "Mona",
    "Ali", "Hassan", "Mariam", "Ziad", "Salma", "Tarek", "Aya", "Karim",
    "Layla", "Dina", "Mostafa", "Rana", "Hany", "Sherif", "Yasmin", "Amr",
    "Farah", "Habiba", "Mahmoud", "John", "Emma", "Lucas", "Sophia", "Liam", "Olivia"
]

LAST_NAMES = [
    "Ibrahim", "Ali", "Hassan", "Khalil", "Mahmoud", "Samir", "Farouk", "Nabil",
    "Mostafa", "Kamal", "Adel", "Fouad", "El-Sayed", "Ghanem", "Metwally", "Othman",
    "Badawi", "Shawky", "Smith", "Johnson", "Brown", "Taylor", "Miller"
]

CITIES = [
    "Cairo", "Alexandria", "Giza", "Mansoura", "Tanta", "Aswan", "Suez",
    "Ismailia", "Port Said", "Luxor", "Hurghada", "Sharm El-Sheikh", "Zagazig",
    "Fayoum", "Beni Suef", "Minya", "Sohag", "Qena"
]

CATEGORIES = [
    "Electronics", "Home & Kitchen", "Fashion", "Books & Stationery",
    "Sports & Fitness", "Beauty & Personal Care", "Toys & Games", "Automotive",
    "Grocery & Gourmet", "Office Supplies", "Health & Wellness", "Pet Supplies"
]

DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com"]

CSV_HEADERS = [
    "transaction_id", "customer_id", "customer_name", "customer_email",
    "store_city", "product_category", "quantity", "unit_price",
    "transaction_date", "ingestion_timestamp"
]

# =============================================================================
# Step 1: Customer Master Pool Generation
# =============================================================================
logging.info(" >>> Generating realistic Customer Master Data... <<< ")
NUM_CUSTOMERS = 30000
CUSTOMER_POOL = []

for cid in range(1001, 1001 + NUM_CUSTOMERS):
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)
    CUSTOMER_POOL.append({
        "customer_id": cid,
        "name": f"{fn} {ln}",
        "email": f"{fn.lower()}.{ln.lower()}{cid}@{random.choice(DOMAINS)}",
        "city": random.choice(CITIES)
    })

logging.info(f"Customer pool generated successfully with {len(CUSTOMER_POOL):,} unique profiles.")


# =============================================================================
# Step 2: Realistic Noise & Anomaly Injectors
# =============================================================================

def apply_text_noise(text, noise_rate=0.08):
    """
    Simulates human data entry noise:
    - 40% of noise: Whitespace padding (leading/trailing).
    - 30% of noise: Lowercase conversion.
    - 30% of noise: Uppercase conversion.
    """
    p = random.random()
    if p < noise_rate * 0.4:
        return f"  {text}  "
    elif p < noise_rate * 0.7:
        return text.lower()
    elif p < noise_rate:
        return text.upper()
    return text


def inject_dirty_email(email):
    """
    Simulates incomplete records and dirty email tokens (~3% anomaly rate):
    - 1.5% chance: Empty string.
    - 1.5% chance: String literal 'NULL'.
    - 3.0% chance: Whitespace padding around valid email.
    """
    p = random.random()
    if p < 0.015:
        return ""
    elif p < 0.03:
        return "NULL"
    elif p < 0.06:
        return f" {email} "
    return email


def inject_dirty_quantity():
    """
    Simulates transactional quantity anomalies:
    - 1% chance: 0 (system processing glitch).
    - 1% chance: -1 (unhandled refund/return flag).
    - 98% chance: Standard purchase volume between 1 and 8 units.
    """
    p = random.random()
    if p < 0.01:
        return 0
    elif p < 0.02:
        return -1
    return random.randint(1, 8)


def inject_dirty_price():
    """
    Simulates pricing anomalies:
    - 1% chance: 0.00 (system freebie or zero-cost bug).
    - 0.5% chance: Negative price (-20.00).
    - 98.5% chance: Standard product price between 10.0 and 1500.0.
    """
    p = random.random()
    if p < 0.01:
        return 0.00
    elif p < 0.015:
        return -20.00
    return round(random.uniform(10.0, 1500.0), 2)


def inject_dirty_date(base_date):
    """
    Generates transactional dates spanning ~600 days from base_date.
    Introduces mixed date formatting:
    - 8% chance: Slash-separated DD/MM/YYYY.
    - 92% chance: Standard ISO YYYY-MM-DD.
    """
    random_days = random.randint(0, 600)
    dt = base_date + timedelta(days=random_days)
    if random.random() < 0.08:
        return dt.strftime("%d/%m/%Y")
    return dt.strftime("%Y-%m-%d")


# =============================================================================
# Step 3: High-Performance Chunked File Generation
# =============================================================================

def generate_bronze_csv(file_path="bronze_transactions_raw.csv", total_rows=1000000, batch_size=50000):
    """
    Generates synthetic transaction records and writes them to disk in batches.

    Args:
        file_path (str): Output CSV filename/path.
        total_rows (int): Total number of transaction rows to produce.
        batch_size (int): Chunk size written to disk per cycle to manage memory.
    """
    logging.info(f"--- Generating {total_rows:,} rows with LOGICAL relationships ---")
    start_total = time.time()
    base_date = datetime(2024, 1, 1)

    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)

        current_txn_id = 1
        rows_written = 0

        while rows_written < total_rows:
            batch = []
            chunk_size = min(batch_size, total_rows - rows_written)
            now_iso = datetime.utcnow().isoformat()

            for _ in range(chunk_size):
                # Sample consistent customer identity
                cust = random.choice(CUSTOMER_POOL)

                # Simulate duplicate primary keys (~0.1% chance)
                txn_id = current_txn_id
                if random.random() < 0.001 and current_txn_id > 1:
                    txn_id = current_txn_id - 1
                else:
                    current_txn_id += 1

                # Construct noisy raw transaction row
                row = [
                    txn_id,
                    cust["customer_id"],
                    apply_text_noise(cust["name"]),
                    inject_dirty_email(cust["email"]),
                    apply_text_noise(cust["city"]),
                    apply_text_noise(random.choice(CATEGORIES), noise_rate=0.05),
                    inject_dirty_quantity(),
                    inject_dirty_price(),
                    inject_dirty_date(base_date),
                    now_iso
                ]
                batch.append(row)

            # Flush batch to disk
            writer.writerows(batch)
            rows_written += len(batch)
            logging.info(f"Progress: {rows_written:,} / {total_rows:,} rows written...")

    duration = time.time() - start_total
    logging.info(f"Done in {duration:.2f}s! Data is now completely logical.")
    logging.info(f"CSV file saved to: {os.path.abspath(file_path)}")


# =============================================================================
# Execution Entry Point
# =============================================================================
if __name__ == "__main__":
    generate_bronze_csv()