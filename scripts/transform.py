from sqlalchemy import create_engine, text
import sys
import os
DB_USER = "sahibjotsingh"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "job_market"

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_DIR = os.path.join(BASE_DIR, "sql")

TRANSFORM_SQL_PATH = os.path.join(SQL_DIR, "transform_staging_to_jobs.sql")
DQ_CHECKS_SQL_PATH = os.path.join(SQL_DIR, "data_quality_checks.sql")


def read_sql_file(path: str) -> str:
    if not os.path.exists(path):
        raise FileNotFoundError(f"SQL file not found: {path}")
    with open(path, "r") as file:
        return file.read()

def pipeline():
    engine = create_engine(DB_URL)

    transform_sql = read_sql_file(TRANSFORM_SQL_PATH)
    dq_checks_sql = read_sql_file(DQ_CHECKS_SQL_PATH)

    with engine.begin() as conn:
        print("RUNNING TRANSFORMATION...")
        conn.execute(text("TRUNCATE TABLE jobs RESTART IDENTITY;"))
        conn.execute(text(transform_sql))
        print("TRANSFORMATION COMPLETE.")

        print("RUNNING DATA QUALITY CHECKS...")
        dq_result = conn.execute(text(dq_checks_sql)).fetchone()

        staging_count = dq_result[0]                        
        jobs_count = dq_result[1]
        null_critical_columns = dq_result[2]
        invalid_salary = dq_result[3]

        if null_critical_columns != 0 or invalid_salary != 0:
            raise ValueError(
                "DATA QUALITY CHECKS FAILED"
                f" | Null critical columns: {null_critical_columns}"
                f" | Invalid salary rows: {invalid_salary}"
            )

        if staging_count != jobs_count:
            raise ValueError(
                "DATA QUALITY CHECKS FAILED"
                f" | Row count mismatch: staging_jobs({staging_count}) != jobs({jobs_count})"
            )

        print("DATA QUALITY CHECKS PASSED.")


if __name__ == "__main__":
    try:
        pipeline()
        print("Pipeline completed successfully.")
    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)
