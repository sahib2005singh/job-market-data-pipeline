Job Market Data Pipeline (End-to-End)

An automated ETL system designed to collect, clean, and store job market data for analytics. This project demonstrates the transition from a manual data script to a production-ready pipeline using industry-standard orchestration.
🚀 Project Overview

This pipeline solves the problem of fragmented job market data. It automates the collection of job postings, standardizes messy inputs, and loads them into a structured PostgreSQL database for analysis.
Key Capabilities

    Automated ETL: Orchestrated via Apache Airflow to handle data flow, dependencies, and retries.

    Data Normalization: Converts raw, unstructured job descriptions into clean, queryable SQL tables using Pandas.

    Containerized Stack: Developed using Docker to ensure consistent environments across development and deployment.

    Error Handling: Built-in logging and task monitoring to manage pipeline failures.

🏗️ Architecture

    Ingestion: Automated ingestion of job posting datasets (Kaggle).

    Processing: Data cleaning and normalization (handling nulls, salary parsing) using Pandas.

    Storage: Relational storage in PostgreSQL.

    Orchestration: Airflow DAGs manage the schedule, task dependencies, and workflow monitoring.

🛠️ Tech Stack

    Languages: Python (Pandas, SQLAlchemy)

    Database: PostgreSQL

    Orchestration: Apache Airflow

    Tools: Docker, Git

📂 Project Structure
Plaintext

job-market-data-pipeline/
│
├── dags/
│   └── job_market_pipeline.py      # Airflow DAG definition
│
├── scripts/
│   ├── clean.py                    # Data cleaning logic
│   ├── load_to_staging_db.py       # SQL Loading scripts
│   └── transform.py                # Business logic & normalization
│
├── data/
│   ├── raw/                        # Original Kaggle datasets
│   └── processed/                  # Cleaned CSVs prior to SQL load
│
├── docker-compose.yml              # Airflow & Postgres services
└── README.md

🚦 Getting Started
1. Clone the repo
Bash

git clone https://github.com/sahib2005singh/job-data-pipeline.git
cd job-data-pipeline

2. Initialize Environment

Ensure you have Docker installed, then run:
Bash

docker-compose up -d

3. Run Pipeline

    Access the Airflow UI at http://localhost:8080.

    Credentials: * Username: admin

        Password: admin

    Locate and trigger the job_market_pipeline_dag.

✅ What This Project Demonstrates

    Pipeline Reliability: Designing fault-tolerant ETL workflows.

    Orchestration: Using Airflow for complex task scheduling beyond simple scripts.

    Data Engineering Best Practices: Modular code structure and environment isolation.

    Portability: Writing Docker-portable workflows that run anywhere.

👤 Author

Sahibjot Singh Computer Science Undergraduate | Aspiring Data Engineer

    GitHub: github.com/sahib2005singh

    LinkedIn: https://www.linkedin.com/in/sahibjot-singh-4a10ab27b/