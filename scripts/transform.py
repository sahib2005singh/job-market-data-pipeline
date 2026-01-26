from sqlalchemy import create_engine, text
from airflow.hooks.base import BaseHook
import sys


CONN_ID = "job_market_postgres"

def read_sql(path):
    with open(path) as f:
        return f.read()

def pipeline():
    conn = BaseHook.get_connection(CONN_ID)

    db_url = (
        f"postgresql://{conn.login}:{conn.password}"
        f"@{conn.host}:{conn.port}/{conn.schema}"
    )

    engine = create_engine(db_url)

    transform_sql = read_sql("/opt/airflow/sql/transform_staging_to_jobs.sql")
    dq_sql = read_sql("/opt/airflow/sql/data_quality_checks.sql")

    with engine.begin() as db:
        print("RUNNING TRANSFORMATION...")
        db.execute(text("TRUNCATE TABLE jobs RESTART IDENTITY;"))
        db.execute(text(transform_sql))
        print("TRANSFORMATION COMPLETE.")

        print("RUNNING DATA QUALITY CHECKS...")
        dq = db.execute(text(dq_sql)).fetchone()

        staging_count, jobs_count, nulls, invalid_salary = dq

        if staging_count != jobs_count:
            raise ValueError("Row count mismatch")

        if nulls != 0 or invalid_salary != 0:
            raise ValueError("DQ checks failed")

        print("DATA QUALITY CHECKS PASSED.")

if __name__ == "__main__":
    try:
        pipeline()
    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)
