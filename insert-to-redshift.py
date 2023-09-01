from first_import import whole_enchilada
import sys
from datetime import datetime
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator


args={'owner': 'airflow'}

default_args = {
 'owner': 'airflow',    
 #'start_date': airflow.utils.dates.days_ago(2),
 # 'end_date': datetime(),
 # 'depends_on_past': False,
 #'email': ['airflow@example.com'],
 #'email_on_failure': False,
 # 'email_on_retry': False,
 # If a task fails, retry it once after waiting
 # at least 5 minutes
 'retries': 1,
 'retry_delay': timedelta(minutes=5),
 }

with DAG(
    dag_id='redshift-inserts',
    schedule=None,
    start_date=datetime(year=2023, month=8, day=24),
    catchup=False
) as dag:

 #No dag.test() in prod
 dag.test()

 task_import_to_pgsql = PythonOperator(
        task_id='json-to-pgsql',
        python_callable=whole_enchilada,
        do_xcom_push=True
    )
 task_mv_used_file = BashOperator(
        task_id='move-old-file',
        bash_command="mv /home/sandbox/airflow/dags/datasets/pickup/*  /home/sandbox/airflow/dags/datasets/done/"
    )
task_import_to_pgsql.set_downstream(task_mv_used_file)
