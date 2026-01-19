from sqlalchemy import create_engine, text
import sys

db_user = 'sahibjotsingh'
db_password = 'postgres'
db_host = 'localhost'
db_port = '5432'
db_name = 'job_market'
db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'


transform_sql_path = 'sql/transform_staging_to_jobs.sql'
dq_checks_sql_path = 'sql/data_quality_checks.sql'

def read_sql_file(path):
    with open(path, 'r') as file:
        return file.read()
    
def pipeline():
    engine = create_engine(db_url)
    transform_sql = read_sql_file(transform_sql_path)
    dq_checks_sql = read_sql_file(dq_checks_sql_path)

    with engine.begin() as conn:
        print("RUNNING TRANSFORMATION...")
        conn.execute(text('truncate table jobs restart identity;'))
        conn.execute(text(transform_sql))
        print("TRANSFORMATION COMPLETE.")

        print("RUNNING DATA QUALITY CHECKS...")
        dq_result = conn.execute(text(dq_checks_sql)).fetchone()
        

        staging_count = dq_result[0]
        jobs_count = dq_result[1]
        null_critical_columns = dq_result[2]
        invalid_salary = dq_result[3]

        if null_critical_columns != 0 or invalid_salary != 0:
            raise ValueError("DATA QUALITY CHECKS FAILED"
                             f" - Nulls in critical columns: {null_critical_columns},"
                             f" Invalid salary entries: {invalid_salary}")
        print("DATA QUALITY CHECKS PASSED.")

if __name__ == "__main__":
    try:
        pipeline()
    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)
    print("Pipeline completed successfully.")
            

