INSERT INTO jobs (
    job_title,
    job_family,
    seniority,
    specialization,
    company,
    company_score,
    city,
    state,
    country,
    is_remote,
    salary_min,
    salary_max,
    date_since_posted
)
SELECT
    job_title,
    job_family,
    seniority,
    specialization,
    company,
    company_score,
    city,
    state,
    country,
    is_remote,
    least(salary_min, salary_max) AS salary_min,
    greatest(salary_min, salary_max) AS salary_max,
    date_since_posted::INTEGER  
FROM staging_jobs;