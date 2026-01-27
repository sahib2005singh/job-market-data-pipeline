Job Market Data Pipeline (End-to-End)

An automated ETL system designed to collect, clean, and store job market data for analytics. This project demonstrates the transition from a manual data script to a production-ready pipeline using industry-standard orchestration.
🚀 Project Overview

This pipeline solves the problem of fragmented job market data. It automates the collection of job postings, standardizes messy inputs (like varying salary formats), and loads them into a structured PostgreSQL database for analysis.
Key Capabilities

    Automated ETL: Orchestrated via Apache Airflow to handle data flow and retries.

    Data Normalization: Converts raw, unstructured job descriptions into clean, queryable SQL tables.

    Containerized Stack: Developed using Docker to ensure consistent environments across development and deployment.

    Error Handling: Built-in logging and task monitoring to manage pipeline failures.

🏗️ Architecture

    Ingestion: kaggle dataset.

    Processing: Data cleaning and normalization using Pandas .

    Storage: Relational storage in PostgreSQL .

    Orchestration: Airflow DAGs manage the schedule and task dependencies.

🛠️ Tech Stack

    Languages: Python (Pandas, SQLAlchemy)

    Database: PostgreSQL

    Orchestration: Apache Airflow

    Tools: Docker, Git

📂 Project Structure

    /dags: Airflow DAG definitions for pipeline scheduling.

    /scripts: Python modules for data extraction and transformation logic.

    /sql: Database schema definitions and setup scripts.

    docker-compose.yml: Configuration for the Airflow and Postgres environment.

🚀 Getting Started

    Clone the repo:
    Bash

    git clone https://github.com/yourusername/job-data-pipeline.git
    cd job-data-pipeline

    Initialize Environment:
    Bash

    docker-compose up -d


    Run Pipeline: Access the Airflow UI at localhost:8080(Username: admin Password: admin) and trigger the job_market_pipeline_dag DAG.
    

🗂 Project Structure
job-market-data-pipeline/
│
├── dags/
│   └── job_market_pipeline.py
│
├── scripts/
│   ├── clean.py
│   ├── load_to_staging_db.py
│   └── transform.py
│
├── data/
│   ├── raw/
│   └── processed/
│
├── docker-compose.yml
└── README.md

What This Project Demonstrates

Designing reliable ETL pipelines

Using Airflow beyond basic DAGs

Applying real-world data engineering practices

Writing Docker-portable data workflows


👤 Author

Sahibjot Singh
Computer Science Undergraduate | Aspiring Data Engineer
GitHub: github.com/sahib2005singh

