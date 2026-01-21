import pandas as pd
from sqlalchemy import create_engine, text


DB_USER = "sahibjotsingh"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "job_market"

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
CSV_PATH = "/Users/sahibjotsingh/Desktop/job-market-data-pipeline/data/processed/jobs_cleaned.csv"

def main():
    print("Reading cleaned data...")
    df = pd.read_csv(CSV_PATH)
    print(f"Rows in CSV: {len(df)}")

    df = df[
        [
            "Job Title",
            "job family",
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

    engine = create_engine(DB_URL)

    
    print("Truncating staging_jobs...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE staging_jobs;"))

    
    print("Loading data into staging_jobs...")
    df.to_sql(
        "staging_jobs",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )

    print(f"Inserted {len(df)} rows into staging_jobs")

if __name__ == "__main__":
    main()
