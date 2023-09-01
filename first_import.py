import json
import sys
import os
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


def whole_enchilada():

 os.chdir('/home/sandbox/airflow/dags')

 try:
  conn_hook = PostgresHook(
     postgres_conn_id='pg_for_me',
     schema='bls'
     )
 except Exception as e:
  print('cannot get pgHook')
  print(e)
  sys.exit()

 try:
  json_data = open('./datasets/pickup/counties.json')
 except Exception as e:
  print("cannot open json file")
  print(e)
  sys.exit()

 try:
  conn = conn_hook.get_conn()
  cursor = conn.cursor()
 except Exception as e:
  print('cannot log into postgres')
  print(e)
  sys.exit()

 try:
  chunky = json.load(json_data)
 except Exception as e:
  print("cannot parse json")
  print(e)
  sys.exit()

 loop_len = len(chunky)

 for step in range(0,loop_len,1):
  print(step)
  primary_key =  chunky[step]['fips']

  county_info = {'name': chunky[step]['name'].replace("'",""),'fips': chunky[step]['fips'], 'state': chunky[step]['state'], 'land_area': chunky[step]['land_area (km^2)'],
               'area': chunky[step]['area (km^2)'],'longitude': chunky[step]['longitude (deg)'], 'latitude': chunky[step]['latitude (deg)'],
               'noaa': chunky[step]['noaa'], 'zip-codes': chunky[step]['zip-codes'] }
  race = chunky[step]['race']
  age = chunky[step]['age']
  male = chunky[step]['male'] ##
  female =  chunky[step]['female'] ##
  population =  chunky[step]['population']
  deaths = chunky[step]['deaths']
  try:
    bls = "'" + json.dumps(chunky[step]['bls']) + "'"
  except:
    bls = 'NULL'
  life_expectancy = chunky[step]['life-expectancy']
  fatal_police_shootings = chunky[step]['fatal_police_shootings']
  police_deaths = chunky[step]['police_deaths'] ##
  avg_income = chunky[step]['avg_income']  ##
  covid_deaths = chunky[step]['covid-deaths']
  covid_confirmed = chunky[step]['covid-confirmed']
  covid_vaccination = chunky[step]['covid-vaccination']
  try:
    elections = "'" + json.dumps(chunky[step]['elections']) + "'"
  except:
    elections = 'NULL'

  education = chunky[step]['edu']
  try:
     cost_of_living = json.dumps(chunky[step]['cost-of-living'])
  except:
     cost_of_living = 'NULL'
  try:
    poverty_rate = chunky[step]['poverty-rate'] ##
  except:
    poverty_rate = 'NULL'
  try:
     industry = "'" + json.dumps(chunky[step]['industry']) + "'"
  except:
     industry = 'NULL'
  try:
    health =  "'" + json.dumps(chunky[step]['health']) + "'"
    if health['Violent Crime Rate'] == 'NaN':
        health['Violent Crime Rate'] = 0
        print('broken violent crime rate')
        sys.exit()
  except:
    health = 'NULL'
  ins_str = "INSERT INTO public.public_statistics (county_fips, county_info, race, age, male, female, population, deaths, bls, life_expectancy, fatal_police_shootings, police_deaths, avg_income, covid_deaths, covid_confirmed, covid_vaccination, elections, education, poverty_rate, cost_of_living, industry, health) values ({},'{}','{}','{}',{},{},'{}','{}',{},{},'{}',{},{},'{}','{}','{}',{},'{}',{},'{}',{},{})  ON CONFLICT (county_fips) DO NOTHING".format(primary_key,json.dumps(county_info), json.dumps(race),json.dumps(age), json.dumps(male),json.dumps(female), json.dumps(population),json.dumps(deaths),bls,json.dumps(life_expectancy),json.dumps(fatal_police_shootings),police_deaths,avg_income,json.dumps(covid_deaths),json.dumps(covid_confirmed),json.dumps(covid_vaccination),elections, json.dumps(education), poverty_rate, cost_of_living, industry, health)

  try:
    cursor.execute(ins_str)
  except Exception as e:
    print('cannot insert to table')
    print(e)
    print(ins_str)
    sys.exit()
 return()
