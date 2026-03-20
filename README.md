# 🔄 Python ETL Pipeline

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-D71F00?style=for-the-badge&logo=python&logoColor=white)
![Azure](https://img.shields.io/badge/Azure_Blob-0089D6?style=for-the-badge&logo=microsoft-azure&logoColor=white)

A modular, production-ready ETL (Extract, Transform, Load) pipeline built in Python. Designed to handle real-world data ingestion workflows — from multiple source types through configurable transformations to flexible target destinations.

---

## 📌 Features

- **Multiple source types** — SQL databases, CSV files, REST APIs
- **Configurable transformations** — cleaning, validation, type casting, deduplication, business rules
- **Flexible targets** — SQL databases, CSV output, Azure Blob Storage
- **Structured logging** — timestamped logs to console and file
- **YAML-based config** — no hardcoded values, environment-friendly
- **Unit tested** — pytest test suite covering core transformation logic

---

## 🗂️ Project Structure

```
python-etl-pipeline/
│
├── src/
│   ├── extractor.py        # Data extraction (SQL, CSV, API)
│   ├── transformer.py      # Data cleaning, validation, transformation
│   └── loader.py           # Data loading (SQL, CSV, Azure Blob)
│
├── config/
│   └── config.yaml         # Pipeline configuration
│
├── tests/
│   └── test_transformer.py # Unit tests
│
├── pipeline.py             # Main pipeline orchestrator
├── requirements.txt
└── .gitignore
```

---

## ⚙️ Configuration

Edit `config/config.yaml` to set your source, transformation rules, and target:

```yaml
source:
  source_type: csv
  file_path: data/sample_input.csv

transform:
  required_columns: [id, name, created_at]
  date_columns: [created_at]
  numeric_columns: [amount]

target:
  target_type: csv
  output_path: data/output/transformed.csv
```

Supports SQL and Azure Blob targets — see `config/config.yaml` for commented examples.

---

## 🚀 Getting Started

```bash
# Clone the repo
git clone https://github.com/syedparvez15/python-etl-pipeline.git
cd python-etl-pipeline

# Install dependencies
pip install -r requirements.txt

# Run the pipeline
python pipeline.py

# Run tests
pytest tests/
```

---

## 🧪 Running Tests

```bash
pytest tests/ -v
```

---

## 📋 Supported Source & Target Types

| Type | Source | Target |
|------|--------|--------|
| SQL Database | ✅ | ✅ |
| CSV / Flat File | ✅ | ✅ |
| REST API (JSON) | ✅ | ❌ |
| Azure Blob Storage | ❌ | ✅ |

---

## 🛠️ Built With

- [Python 3.10+](https://www.python.org/)
- [Pandas](https://pandas.pydata.org/)
- [SQLAlchemy](https://www.sqlalchemy.org/)
- [Azure Storage Blob SDK](https://pypi.org/project/azure-storage-blob/)
- [PyYAML](https://pyyaml.org/)

---

## 👤 Author

**Syed Parvez** — [LinkedIn](https://www.linkedin.com/in/syedparvez15/) | [GitHub](https://github.com/syedparvez15)

