SELECT
    (SELECT COUNT(*) FROM staging_jobs) AS staging_count,
    (SELECT COUNT(*) FROM jobs) AS jobs_count,
    (SELECT COUNT(*) FROM jobs
        WHERE job_title IS NULL
           OR company IS NULL
           OR job_family IS NULL
    ) AS null_critical_columns,
    (SELECT COUNT(*) FROM jobs
        WHERE salary_min IS NOT NULL
          AND salary_max IS NOT NULL
          AND salary_min > salary_max
    ) AS invalid_salary_rows;
