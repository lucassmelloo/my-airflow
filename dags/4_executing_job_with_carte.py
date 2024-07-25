
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow_pentaho.operators.carte import CarteJobOperator
from airflow.decorators import task

with DAG(
    dag_id='4_executing_job_with_carte',
    description='Trying to execute a Job using CarteJobOperator',
    start_date=days_ago(2),
    schedule_interval='@daily'
) as dag:

    tarefa_4 = BashOperator(
        task_id='create_dag_folder',
        bash_command = 'mkdir -p /opt/airflow/shared/'
    )
    tarefa_5 = CarteJobOperator(
        pdi_conn_id='pentaho_server',
        task_id='executing_dag_with_airflow' ,
        job="/Carga_VendasDiarias_e_Orcamento.kjb"
    )

    @task
    def execute_carte_job_operator():
        URL = 'cluster:cluster@10.35.101.31:8081'

    tarefa_4 >> tarefa_5
