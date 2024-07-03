from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

with DAG(
    '1_primeiro_dag',
    start_date=days_ago(2),
    schedule_interval='@daily'
) as dag:
    
    tarefa_1 = EmptyOperator(task_id='tarefa_1')
    tarefa_2 = EmptyOperator(task_id='tarefa_2')
    tarefa_3 = EmptyOperator(task_id='tarefa_3')

    tarefa_4 = BashOperator(
        task_id='create_dag_folder',
        bash_command = 'mkdir -p /opt/airflow/shared/primeiro_dag'
    )
    tarefa_5 = BashOperator(
        task_id='create_folder_task',
        bash_command = 'mkdir -p /opt/airflow/shared/primeiro_dag/{{data_interval_end}}'
    )

    tarefa_1 >> [tarefa_2,tarefa_3]
    tarefa_3 >> tarefa_4
    tarefa_4 >> tarefa_5
