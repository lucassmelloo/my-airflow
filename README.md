# Airflow Project

## Abstract

This project contains a collection of DAGs created for study purposes. Each DAG has its own description and minimum requirements such as connections, variables, and environment variables. The Airflow instance is running in Docker mode. In addition to starting the basic services such as the Webserver, Scheduler, Postgres, Triggerer, Redis, and Worker, I am also bringing up a MySQL database on port 3007.

## DAG's
### 1_my_first_dag

This DAG has the simple objective of creating a folder inside `shared/1_my_first_dag` and learn a little more about tasks orchestration. The name of the folder is the execution date of the job.
As a case study of task organization, 5 tasks were created, 3 of wich were EmptyOperators and the other two were BashOperators to create the folders.

#### Tasks:
  - Task 1: EmptyOperator, the empty operator has no one responsability.
  - Task 2: EmptyOperator [Depends of "Task 1"]
  - Task 3: EmptyOperator [Depends of "Task 1"]
  - Create DAG Folder: BashOperator, this task is responsible for creating the main folder that conatains the folders that will be created by the task [Depends of "Task 3"]
  - Create Folder Task: BashOperator, this it will create the folder with the date of execution of job [Depends of Task "Create DAG Folder"]
