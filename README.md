# Airflow Project

## Abstract

This project contains a collection of DAGs created for study purposes. Each DAG has its own description and minimum requirements such as connections, variables, and environment variables. The Airflow instance is running in Docker mode. In addition to starting the basic services such as the Webserver, Scheduler, Postgres, Triggerer, Redis, and Worker, I am also bringing up a MySQL database on port 3007.

## Dags
### 1_my_first_dag

This DAG has the simple objective of creating a folder inside `shared/1_my_first_dag`. The name of the folder is the execution date of the job.
