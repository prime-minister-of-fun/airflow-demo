from row_by_row1 import row_by_agonizing_row
from row_by_row2 import  do_snowflake_files
import sys
import os
from datetime import datetime
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.models.xcom import XCom
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow import AirflowException
from airflow.exceptions import AirflowProviderDeprecationWarning
#from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from  airflow.providers.postgres.hooks.postgres import PostgresHook

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
    dag_id='row-by-row',
    schedule=None,
    default_args=default_args,
    start_date=datetime(year=2023, month=8, day=24),
    catchup=False
) as dag:
 def simple_exporter():
  @task
  def get_the_row():
    the_file = row_by_agonizing_row()
    return {'file_name': the_file}
  @task(multiple_outputs=True)
  def the_snowflake_files(file_name):
    pg_file = do_snowflake_files(file_name)
    return {'pg_file_name': pg_file}
  @task
  def the_pg_files(pg_file_name):
   print("checking for pg file passed")
   print(pg_file_name)

   try:
    conn_op = PostgresHook(
    postgres_conn_id='pg_for_me',
    schema='bls',
    autocommit=True
    )
   except Exception as e:
    print('cannot get postgresHook')
    raise AirflowException(e)

   try:
    conn = conn_op.get_conn()
    cursor = conn.cursor()
   except Exception as e:
    print("Could not get cursor")
    raise AirflowException(e)

   os.chdir('/home/mpapet/airflow/dags/datasets/pickup')
   process_file_list = list()

   all_files_list = os.listdir("./")
   #KISS any file here has not been imported to snowflake

   for i in all_files_list:
    if i.startswith('update_row_') and i not in process_file_list:
     process_file_list.append(i)

   print(process_file_list)
   print("length of file_list {}".format(len(process_file_list)))
   if len(list(process_file_list)) == 0:
    print('postgresql file list is empty')
    return{"pg_files": "empty"}
   for f in list(process_file_list):
    file_stats = os.stat(f)
    print("processing {}".format(f))
    if file_stats.st_size > 10:
     fh = open(f, "r+")
     qry = fh.read()
     print("!!!!!!!!!!!! DEBUG !!!!!!!!!!!!!!!")
     print(qry)
     cursor.execute(qry)
     fh.close()
    else:
     #should never be here.
     print("{} is too small??  {}".format(f,file_stats.st_size))
   return{"pg_files":"|".join(process_file_list)}
  #@task
  #def move_snowflake():
  # task_mv_used_snowflake = BashOperator(
  #  task_id='move-snowflake-queries',
  #  bash_command="mv /home/mpapet/airflow/dags/datasets/pickup/*_move_row.sql  /home/mpapet/airflow/dags/datasets/done/"
  #  )
 #  print("result from snowflake bashOperator {}".format(task_mv_used_snowflake))
 #  return{"snowflake_moved": 'True'}

 # @task
 # def move_pgsql():
 #  task_mv_used_update_pgsql = BashOperator(
 #   task_id='move-pgsql-date-exported-update',
 #   bash_command="mv /home/mpapet/airflow/dags/datasets/pickup/update_row_*.sql  /home/mpapet/airflow/dags/datasets/done/"
 #   )
 #  print("result from pgsql bashOperator {}".format(task_mv_used_update_pgsql))
 #  return{"pgsql_moved": 'True'}
  row_data = get_the_row()
  snowflake_rows = the_snowflake_files(row_data)


 simple_exporter()
