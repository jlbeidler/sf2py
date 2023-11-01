#!/bin/csh -f

#/usr/bin/python3 -u ./load_source.py datasources/2023/hms_nrt_2023.json
#sleep 1h
/usr/bin/python3 -u ./reconcile_fires.py streams/nrt2023.json
