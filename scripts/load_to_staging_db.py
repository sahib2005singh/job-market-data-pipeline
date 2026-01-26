import os

import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import traceback

BASE_DIR = Path("/opt/airflow")
CSV_PATH = BASE_DIR / "data" / "processed" / "jobs_cleaned.csv"

def main():
    print("Reading cleaned data...", flush=True)
    df = pd.read_csv(CSV_PATH)
    print(f"Rows in CSV: {len(df)}", flush=True)

    df = df[
        [
            "Job Title",
            "job_family",
            "seniority",
            "specialization",
            "Company",
            "Company Score",
            "city",
            "state",
            "country",
            "is_remote",
            "salary_min",
            "salary_max",
            "date_since_posted",
        ]
    ]

    df.columns = [
        "job_title",
        "job_family",
        "seniority",
        "specialization",
        "company",
        "company_score",
        "city",
        "state",
        "country",
        "is_remote",
        "salary_min",
        "salary_max",
        "date_since_posted",
    ]

    
    db_url = os.environ.get("JOB_MARKET_DB_URL")
    print(f"Database URL: {db_url}", flush=True)
    
    if not db_url:
        raise ValueError("JOB_MARKET_DB_URL environment variable not set!")
    
    engine = create_engine(db_url)

   
    print("Testing database connection...", flush=True)
    with engine.connect() as conn:
        # Check if we can connect
        result = conn.execute(text("SELECT current_database();"))
        db_name = result.scalar()
        print(f"Connected to database: {db_name}", flush=True)
        
        
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'staging_jobs'
            );
        """))
        table_exists = result.scalar()
        print(f"staging_jobs table exists: {table_exists}", flush=True)
        
        if not table_exists:
            
            print("Creating staging_jobs table...", flush=True)
            create_table_sql = """
            CREATE TABLE staging_jobs (
                job_title VARCHAR(255),
                job_family VARCHAR(100),
                seniority VARCHAR(50),
                specialization VARCHAR(100),
                company VARCHAR(255),
                company_score DECIMAL(3,1),
                city VARCHAR(100),
                state VARCHAR(100),
                country VARCHAR(100),
                is_remote BOOLEAN,
                salary_min DECIMAL(10,2),
                salary_max DECIMAL(10,2),
                date_since_posted VARCHAR(50)
            );
            """
            conn.execute(text(create_table_sql))
            conn.commit()
            print("Created staging_jobs table", flush=True)

    print("Truncating staging_jobs...", flush=True)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE staging_jobs;"))

    print("Loading data into staging_jobs...", flush=True)
    df.to_sql(
        "staging_jobs",
        engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )

    print(f"Inserted {len(df)} rows into staging_jobs", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", flush=True)
        traceback.print_exc()