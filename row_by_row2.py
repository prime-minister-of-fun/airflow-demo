import sys
import os
from datetime import datetime
from airflow.models import Variable
from airflow.models.xcom import XCom
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
#AirflowException allows retries
from airflow import AirflowException

def do_snowflake_files(single_file):
  print("passing single_file")
  print(single_file)
  os.chdir('/home/mpapet/airflow/dags/datasets/pickup')
  process_file_list = list()
  all_files_list = os.listdir("./")
  #KISS any file here has not been imported to snowflake
  for i in all_files_list:
   print(i)
   if i.endswith('_move_row.sql') and i not in process_file_list:
    print(i)
    process_file_list.append(i)

  if len(process_file_list) == 0:
   print("no files to process")
   return('')
  else:
   print("sending file list")
   print(process_file_list)

  try:
   snow_op = SnowflakeOperator(
      task_id='insert-to-snowflake',
      snowflake_conn_id='snow_for_me',
      warehouse="COMPUTE_WH",
      database="DEMO_DATA",
      schema="PUBLIC",
      autocommit=True,
    )
  except Exception as e:
   print(e)
   print('error inserting to snowflake')
   raise AirflowException(e)

  snowflake_query = ''
  fips_list = []
  for f in list(process_file_list):
   print("checking {}".format(f))
   file_stats = os.stat(f)
   fips_finder = f.split('_')
   if file_stats.st_size > 10 and fips_finder not in fips_list:
    fips_list.append(str(fips_finder[0]))
    fh = open(f, "r+")
    qry = fh.read()
    print("!!!!!!!!!!!! DEBUG !!!!!!!!!!!!!!!")
    print(qry)
    snow_op.run(qry, autocommit=TRUE, handler="first_snowflake_handle")
   else:
    #should never be here.
    print("{} is too small??  {}".format(f,file_stats.st_size))

  print("snowflake handle returned")
  print(snow_op.first_snowflake_handle)
  timestamp = datetime.now()
  file_name = 'update_row_' + timestamp.strftime("%Y-%m-%d-%H:%M:%S") + ".sql"
  update_query = "UPDATE public.public_statistics set date_exported = CURRENT_TIMESTAMP where county_fips in ({}); \n COMMIT;".format(','.join(fips_list))
  try:
   f = open(file_name, "w")
   f.write(update_query)
   f.close()
  except Exception as e:
   print(e)
   print('error writing pgsql update query')
   raise AirflowException(e)
  print("DEBUG returning {}".format(file_name))
  return(file_name)
