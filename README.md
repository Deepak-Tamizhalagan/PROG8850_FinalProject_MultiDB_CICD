# PROG8850 – Final Group Project
# Cross-Database Automation, Monitoring & Anomaly Detection with CI/CD

Team Members: 
+ Deepak (CI/CD), 
+ Cibi Sharan (Monitoring), 
+ Altaf (Anomaly Detection)

## Task- 1 CI/CD Pipeline for Multi-Database Deployment
For this project, my primary responsibility was to set up the CI/CD pipeline and automate the database. 

The aim of my part was to automate the deployment of MySQL and MongoDB, run the ETL process directly from the pipeline, and validate data consistency between both databases using Python.

## What I Did (Step-by-Step)

I started by creating the GitHub Actions workflow file (ci_cd_pipeline.yml). I configured it to run automatically on every commit to the main branch.

In the pipeline:

- I set up MySQL and MongoDB containers using GitHub's services feature.

- I then created SQL scripts (create_database.sql and create_tables.sql) to automatically create the database (climate_db) and the weather_data table.

- Next, I wrote the ETL Python script (etl.py) to fetch real climate indicators from the World Bank public API, transform the dataset, and insert it into both MongoDB and MySQL.

- I used concurrent_ops.py to simulate parallel transactions in both databases using Python threading.

- Finally, I implemented validate_consistency.py to compare the record counts and ensure that both systems hold matching data.

## Challenges Faced and How I Solved Them

During execution, I went through multiple failures, especially related to database connections.

| Issue Faced |	What I Initially Tried |	Final Solution |
|-------------|------------------------|-----------------|
| MongoDB was not reachable in CI	| Used normal connection string |	Switched to local container URI and used health check and later mock mode |
Pipeline kept failing during container initialization |	Default settings |	Increased health check time and modified connection commands
MySQL threw error due to NaN values |	Direct insert |	Added data cleaning using Pandas before insertion
MongoDB had TLS and URI encoding issues |	Used full URL from Compass |	Replaced @ with %40 and avoided SSL in CI

Across all these failures, I learned how to debug pipelines systematically, by analyzing logs and making small incremental fixes. This process took multiple iterations but helped me build a stable pipeline that could run successfully without manual intervention.

## Final Outcome

After implementing all fixes and optimizations, my pipeline successfully:
```
✔ Deployed MySQL and MongoDB

✔ Loaded the dataset from the API

✔ Inserted data into both DBs

✔ Simulated concurrent transactions

✔ Validated data consistency
```

The final build took approximately 48 seconds to complete and passed without any errors.
