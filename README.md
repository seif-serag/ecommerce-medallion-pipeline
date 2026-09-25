<div align="center">

# 🛒 E-Commerce Medallion Architecture Data Pipeline
**Production-Grade Data Engineering | 1,000,000 Transactions | Lakehouse Pattern**

<br/>

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Safe%20Serag-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/safe-serag/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![SQLite](https://img.shields.io/badge/SQLite-WAL_Tuned-003B57?style=for-the-badge&logo=sqlite&logoColor=white)](https://www.sqlite.org/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion-E65100?style=for-the-badge)](https://en.wikipedia.org/wiki/Data_lakehouse)
[![Design](https://img.shields.io/badge/Pattern-OOP_Driven-6A1B9A?style=for-the-badge)](https://en.wikipedia.org/wiki/Object-oriented_programming)
[![Scale](https://img.shields.io/badge/Volume-1M_Records-2E7D32?style=for-the-badge)](https://github.com/)

<br/>

| ⚡ Dataset Scale | 🛡️ Data Quality | 🏛️ Modeling Pattern | ⚡ Engine Tuning |
| :---: | :---: | :---: | :---: |
| **1,000,000 Records** | **Automated Quarantine** | **Star Schema & Marts** | **SQLite WAL Mode** |

</div>

---

## 📌 Overview
An end-to-end, high-performance ETL/ELT pipeline processing **1,000,000 transaction records** using the **Medallion Lakehouse Architecture**. The pipeline ingests raw unstructured records, enforces strict schema integrity and data quality quarantining, and models the validated data into an analytics-ready Star Schema and KPI Data Marts using clean Object-Oriented Programming (OOP) principles.

---

## 🏗️ Medallion Pipeline Architecture

| Layer | Stage | Target Tables | Engineering Specs & Responsibilities |
| :---: | :--- | :--- | :--- |
| **🥉** | **Bronze**<br>*(Raw Ingestion)* | `bronze_transactions_raw` | • High-throughput batch streaming from CSV into untyped text staging.<br>• Performance tuning via `PRAGMA synchronous = NORMAL` & `PRAGMA journal_mode = WAL`.<br>• Zero business logic; preserves raw schema-on-read fidelity. |
| **🥈** | **Silver**<br>*(Cleansing & Audit)* | `silver_transactions`<br>`quarantine_audit` | • Strict type coercion, date normalization (`YYYY-MM-DD`), and boundary validations.<br>• Negative quantity/price correction and whitespace sanitization.<br>• **Quarantine Isolation Pattern:** Corrupted rows and duplicate primary keys are safely routed to audit storage without interrupting execution. |
| **🥇** | **Gold**<br>*(Analytics & Marts)* | `fact_sales`<br>`dim_customers`<br>`dim_dates`<br>`mart_city_performance` | • Pure OOP implementation encapsulated in `GoldPipeline` class.<br>• In-memory entity resolution resolving customer master records.<br>• Dimensional Star Schema modeling with pre-computed business aggregation metrics. |

---

## 📊 Dimensional Modeling (Star Schema & Marts)

The Gold layer transforms sanitized Silver records into analytical models:

* **Fact Table (`fact_sales`):** Tracks granular sales metrics (`sale_id`, `customer_id`, `date_key`, `quantity`, `unit_price`, `total_amount`).
* **Dimension Tables:**
  * **`dim_customers`:** Enforces in-memory SCD/entity resolution with unique customer identities, primary city, and registration profiles.
  * **`dim_dates`:** Extracted temporal hierarchy (`date_key`, `year`, `quarter`, `month`, `day`, `day_of_week`).
* **Business Mart (`mart_city_performance`):** Aggregates city-level total revenue, order volume, and average order value (AOV) for BI dashboards.

---

## ⚡ Core Engineering Highlights

* **Resilient Data Quality Guardrails:** Injects simulated anomalies (duplicate PKs, malformed dates, negative amounts) and isolates failures into dedicated quarantine audit tables.
* **Database Engine Tuning:** Optimized SQLite write locks and latency through atomic multi-row commit batches and WAL-mode write concurrency.
* **Modular OOP Architecture:** Clean separation of concerns with structured classes handling extract, validation, state tracking, and dimensional loading.
* **Dual Production Logging:** Unified logging system outputting clean real-time status to console and rotating trace logs (`*.log`) tracking throughput and elapsed time.

---

## 📂 Repository Structure

* **`scripts/CSV_File_Data_Generator.py`** — Simulates 1M realistic e-commerce transactions with injected edge cases and master customer pools.
* **`scripts/Bronze_Layer.py`** — Streams raw files directly into untyped SQLite staging tables with bulk PRAGMA optimizations.
* **`scripts/Silver_Layer.py`** — Executes data quality cleansing, boundary checks, and fault-tolerant audit quarantine routing.
* **`scripts/Gold_Layer.py`** — OOP engine orchestrating entity resolution, star schema population, and KPI summary marts.
* **`logs/`** — Execution traces detailing row-level metrics and latency benchmarks.
* **`.gitignore`** — Enforces strict isolation of raw binaries, local CSVs, and database files.

---

---

## 🙏 Acknowledgements & Mentorship

Special thanks and appreciation to the mentors who provided exceptional educational content, practical industry knowledge, and technical motivation:

* **Baraa Salkini (Data With Baraa):** For master-level teaching on end-to-end Data Engineering concepts, pipeline architectures, Lakehouse Medallion paradigms, and dimensional modeling patterns.
* **Osama Elzero (Elzero Web School):** For being a foundational pillar of programming education, algorithmic thinking, and building dedicated software engineering discipline across the Arab developer community.

---

---

## 👨‍💻 About the Author & Contact

**Safe Serag**  
Data Engineer passionate about building scalable data pipelines, distributed systems, and robust ETL/ELT architectures.

* **LinkedIn:** [Safe Serag](https://www.linkedin.com/in/safe-serag/)
* **GitHub:** [@seif-serag](https://github.com/seif-serag)

Feel free to reach out for collaborations, discussions on data engineering practices, or feedback on this project!
