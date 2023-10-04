import json
import sys
import os
from datetime import datetime
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.models.xcom import XCom
from airflow.providers.postgres.hooks.postgres import PostgresHook
#AirflowException allows retries
from airflow import AirflowException


def row_by_agonizing_row():
  os.chdir('/home/mpapet/airflow/dags/datasets/pickup')
  try:
   conn_hook = PostgresHook(
     postgres_conn_id='pg_for_me',
     schema='bls',
     task_id='select-single-row',
     )
  except Exception as e:
   print('cannot get pgHook')
   raise AirflowException(e)

  try:
   conn = conn_hook.get_conn()
   cursor = conn.cursor()
  except Exception as e:
   print('cannot log into postgres')
   raise AirflowException(e)

  sql_query = "SELECT * FROM public.public_statistics WHERE date_exported IS NULL LIMIT 5"
  list_of_files = list()
  try:
   cursor.execute(sql_query)
   pg_data = cursor.fetchall()
  except Exception as e:
   print('cannot run query')
   print(e)
   raise AirflowException(e)
  update_key = None
 #Q: why is exploding the list necessary?
 #A: detect formatting errors.  "data quality" is pass/fail
  looper = 0
  for row in pg_data:
   county_fips = row[0]
   county_info = row[1]
   race = row[2]
   age = row[3]
   male = row[4]
   female = row[5]
   population = row[6]
   deaths = row[7]
   bls = row[8]
   life_expectancy = row[9]
   fatal_police_shootings = row[10]
   police_deaths = row[11]
   avg_income = row[12]
   covid_deaths = row[13]
   covid_confirmed = row[14]
   covid_vaccination = row[15]
   elections = row[16]
   education = row[17]
   poverty_rate = row[18]
   cost_of_living = row[19]
   industry = row[20]
   health = row[21]

   snowflake_ins = "INSERT INTO public.public_statistics(county_fips, county_info, race, age, male, female, population, deaths, bls, life_expectancy, fatal_police_shootings, police_deaths, avg_income, covid_deaths, covid_confirmed, covid_vaccination, elections, education, poverty_rate, cost_of_living, industry, health, date_exported) VALUES"
   if looper != 0:
     snowflake_ins += ','

   snowflake_ins += "({},\'{}\', \'{}\', \'{}\', {}, {}, \'{}\', \'{}\', \'{}\', {}, \'{}\', {}, {}, \'{}\', \'{}\', \'{}\', \'{}\',\'{}\', {}, \'{}\', \'{}\', \'{}\',CURRENT_TIMESTAMP());".format(county_fips, county_info, race, age, male, female, population, deaths, bls, life_expectancy, fatal_police_shootings, police_deaths, avg_income, covid_deaths, covid_confirmed, covid_vaccination, elections, education, poverty_rate, cost_of_living, industry, health)
   looper += 1

  snowflake_file = 0
  timestamp = datetime.now()
  file_name = str(county_fips) + '_' + timestamp.strftime("%Y-%m-%d-%H:%M:%S") + "_move_row.sql"
  if len(snowflake_ins) > 10:

   try:
    f = open(file_name, "w")
    f.write(snowflake_ins + "\n")
    f.write(";")
    f.close()
    snowflake_file = 1
   except Exception as e:
    print(e)
    print('error writing snowflake query')
    raise AirflowException(e)
  else:
    print("Nothing to sync")
    return("NULL")

  if snowflake_file == 1:
    #context["ti"].xcom_push(key='last_file', value=file_name)
    print("should be successful")
    return(file_name)
  elif len(snowflake_ins) <= 10:
    print('success, but no file to send')
    return('NULL')
  else:
   print("snowflake_file returned {} ?".format(snowflake_file))
   raise AirflowException("snowflake_file returned {} not 1".format(snowflake_file))
  return(file_name)
