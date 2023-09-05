# airflow-demo
demo airflow DAGS

The data dumped into the database is in a tar.gz.  Decompress it.

The table is for a PostgreSQL database with postGIS installed because postGIS and pgRouting are both amazing.

Look at the job before running.  It uses both a dropoff and done directory.  Completed file goes in done.

## Data sources
Data sources are not unique or original, correctness not guaranteed:

American County FIPS and longitude latitude:https://gist.github.com/russellsamora/12be4f9f574e92413ea3f92ce1bc58e6

Public statistics by county: https://github.com/evangambit/JsonOfCounties

This is a better shape file from the U.S. census to determine where in a county a point lies. cb_2018_us_county_500k.shp

The EV data comes from snowflake.  Of course, I do not in any way encourage copyright violation. That means you probably need an account and *perhaps* export the data using an awesome tool like dbeaver to your localhost.
