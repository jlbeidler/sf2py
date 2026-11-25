#!/bin/bash

# Modify the smartfire_tables.sql to match EPSG for study area
#Prior to running create the new postGIS db and grant the user access:
#psql -U postgres
#CREATE DATABASE [dbname];
#\connect [dbname];
#CREATE EXTENSION postgis;
#Optional: CREATE USER [user] WITH PASSWORD '[password]';
#GRANT ALL ON DATABASE [dbname] TO [user];

dbname=sf2
pguser=sf2
host=localhost

script_dir=$(cd "$(dirname "$0")" && pwd)
psql -U "$pguser" -h "$host" -W "$dbname" \
  -f "$script_dir/../src/sf2py/sql/smartfire_tables.sql"
