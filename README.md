Markdown
# 🛒 E-Commerce Medallion Architecture Data Pipeline

An end-to-end, production-grade data engineering pipeline implementing the **Medallion Architecture (Bronze -> Silver -> Gold)** using Python, SQLite, and Object-Oriented Programming (OOP). The system generates, cleanses, quarantines, and transforms 1,000,000 synthetic transaction records into analytics-ready dimensional models.

---

## 🏗️ Architecture Overview

```text
       [Data Generator]
              │ (1M Synthetic Transactions with Injected Anomalies)
              ▼
    ┌───────────────────┐
    │   Bronze Layer    │  Raw ingestion into untyped staging tables (PRAGMA bulk-write tuning)
    └─────────┬─────────┘
              │
              ▼
    ┌───────────────────┐
    │   Silver Layer    │  Type coercion, deduplication, cleansing, and PK quarantine isolation
    └─────────┬─────────┘
              │
              ▼
    ┌───────────────────┐
    │    Gold Layer     │  In-memory entity resolution, Star Schema modeling & Data Mart aggregations
    └───────────────────┘
## 🚀 Key Engineering Highlights
Medallion Paradigm: Strict separation of ingestion (Bronze), quality enforcement (Silver), and business aggregation (Gold).

Streaming & Bulk Ingestion: Chunk-based processing using tuned SQLite engine parameters (journal_mode=WAL, synchronous=NORMAL, and transactions) to ingest 1M rows with minimal latency.

Data Quality & Quarantine Pattern: Fault-tolerant pipeline isolating corrupted primary keys and malformed records into dedicated audit tables without halting execution.

OOP-Driven Gold Engine: Pure object-oriented pipeline design (GoldPipeline) leveraging Python data structures for rapid in-memory entity resolution, star schema population (fact_sales, dim_customers, dim_dates), and data marts (mart_city_performance).

Production Dual Logging: Unified, non-blocking logging streams across both Console (stdout) and rotating trace log files.

## 📁 Repository Structure
Plaintext
├── scripts/
│   ├── CSV_File_Data_Generator.py  # 1M dataset simulator with master customer pool & anomalies
│   ├── Bronze_Layer.py             # High-throughput streamed ingestion to staging tables
│   ├── Silver_Layer.py             # Cleansing, type casting, validation, and audit quarantine
│   └── Gold_Layer.py               # OOP star-schema modeling & business mart aggregations
├── .gitignore                      # Enforces isolation of binaries, databases, and heavy raw files
└── README.md                       # Comprehensive system documentation
### 🛠️ Tech Stack
Language: Python 3.10+

Database Engine: SQLite3 (PRAGMA optimized)

Architecture: Medallion (Data Lakehouse Pattern) & Star Schema (Dimensional Modeling)

Design Pattern: Object-Oriented Programming (OOP)

Standard Libraries: sqlite3, csv, logging, datetime, random

## ⚙️ How to Run Locally
Clone the repository:

Bash
git clone [https://github.com/seif-serag/ecommerce-medallion-pipeline.git](https://github.com/seif-serag/ecommerce-medallion-pipeline.git)
cd ecommerce-medallion-pipeline
Generate the synthetic dataset (1M rows):

Bash
python scripts/CSV_File_Data_Generator.py
Execute the pipeline stages:

Bash
python scripts/Bronze_Layer.py
python scripts/Silver_Layer.py
python scripts/Gold_Layer.py
