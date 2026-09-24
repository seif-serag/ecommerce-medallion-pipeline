<div align="center">

# ecommerce-medallion-pipeline

<p align="center">An end-to-end Medallion Architecture data pipeline built with Python & SQLite, processing 1M records with streaming ingestion, data quality quarantine, and in-memory Gold aggregations.</p>

[![Stars](https://img.shields.io/github/stars/seif-serag/ecommerce-medallion-pipeline?style=flat-square)](https://github.com/seif-serag/ecommerce-medallion-pipeline/stargazers) [![Forks](https://img.shields.io/github/forks/seif-serag/ecommerce-medallion-pipeline?style=flat-square)](https://github.com/seif-serag/ecommerce-medallion-pipeline/network) [![Issues](https://img.shields.io/github/issues/seif-serag/ecommerce-medallion-pipeline?style=flat-square)](https://github.com/seif-serag/ecommerce-medallion-pipeline/issues) [![Watchers](https://img.shields.io/github/watchers/seif-serag/ecommerce-medallion-pipeline?style=flat-square)](https://github.com/seif-serag/ecommerce-medallion-pipeline/watchers) [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=flat-square)](https://opensource.org/licenses/MIT)

![Python](https://img.shields.io/badge/-Python-555?style=flat-square&logo=python) ![SQLite3](https://img.shields.io/badge/-SQLite3-555?style=flat-square&logo=sqlite3) ![Medallion Architecture](https://img.shields.io/badge/-Medallion%20Architecture-555?style=flat-square&logo=medallion%20architecture) ![ETL](https://img.shields.io/badge/-ETL-555?style=flat-square&logo=etl) ![Data Engineering](https://img.shields.io/badge/-Data%20Engineering-555?style=flat-square&logo=data%20engineering) ![Dimensional Modeling](https://img.shields.io/badge/-Dimensional%20Modeling-555?style=flat-square&logo=dimensional%20modeling) ![OOP](https://img.shields.io/badge/-OOP-555?style=flat-square&logo=oop)

[🐛 Report Bug](https://github.com/seif-serag/ecommerce-medallion-pipeline/issues) · [✨ Request Feature](https://github.com/seif-serag/ecommerce-medallion-pipeline/issues)

</div>

---

## 📋 Table of Contents

- [⚙️ Prerequisites](#prerequisites)
- [🚀 Installation](#installation)
- [💻 Usage](#usage)
- [✨ Features](#features)
- [🗺️ Roadmap](#roadmap)
- [🤝 Contributing](#contributing)
- [❓ FAQ](#faq)
- [📄 License](#license)
- [👤 Contact](#contact)
- [🙏 Acknowledgements](#acknowledgements)

## ⚙️ Prerequisites

- Python 3.10+
- SQLite3
- Git

## 🚀 Installation

```bash
git clone https://github.com/seif-serag/ecommerce-medallion-pipeline.git
cd ecommerce-medallion-pipeline
```

## 💻 Usage

```bash
python scripts/CSV_File_Data_Generator.py
python scripts/Bronze_Layer.py
python scripts/Silver_Layer.py
python scripts/Gold_Layer.py
```

## ✨ Features

- ✅ Medallion Architecture (Bronze, Silver, Gold Layers)
- ✅ 1,000,000 synthetic transaction records processing
- ✅ PRAGMA-tuned bulk ingestion with WAL mode
- ✅ Strict data cleansing, type coercion, and anomaly detection
- ✅ Fault-tolerant audit quarantine table for corrupted primary keys
- ✅ OOP-driven Gold layer engine (GoldPipeline class)
- ✅ Star Schema dimensional model (fact_sales, dim_customers, dim_dates)
- ✅ Business aggregations with city performance data marts
- ✅ Unified dual logging to console and persistent trace files

## 🗺️ Roadmap

- [ ] Migrate storage engine from SQLite to PostgreSQL / DuckDB
- [ ] Integrate Apache Airflow for scheduled DAG orchestration
- [ ] Add automated data quality unit tests with Great Expectations
- [ ] Deploy Docker containerization for end-to-end execution
- [ ] Build a Streamlit dashboard on top of Gold Data Marts

See the [open issues](https://github.com/seif-serag/ecommerce-medallion-pipeline/issues) for proposed features and known issues.

## 🤝 Contributing

Contributions are what make the open-source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. Fork the project
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## ❓ FAQ

**Q: How do I get started?**
A: Follow the installation guide above.

**Q: How do I report a bug?**
A: Open an issue on the [GitHub Issues](https://github.com/seif-serag/ecommerce-medallion-pipeline/issues) page.

**Q: Can I contribute?**
A: Yes! See the Contributing section above.

## 📄 License

Distributed under the MIT License. See `LICENSE` for more information.

## 👤 Contact

**seif-serag**
- GitHub: [@seif-serag](https://github.com/seif-serag)
- Email: [https://www.linkedin.com/in/safe-serag-89a8a3253/](mailto:https://www.linkedin.com/in/safe-serag-89a8a3253/)
- Project: [https://github.com/seif-serag/ecommerce-medallion-pipeline](https://github.com/seif-serag/ecommerce-medallion-pipeline)

## 🙏 Acknowledgements

- Medallion Architecture Paradigm (Databricks)
- Python Software Foundation
- SQLite Development Team
- Open-source Data Engineering Community

---

<div align="center">Made with ❤️ by seif-serag</div>
