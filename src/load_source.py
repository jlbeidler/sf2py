#!/usr/bin/env python3

import sys
import importlib
from sources import DataSource
from database import DataBase

a = DataSource(sys.argv[1])
db = DataBase('config/pg.json')
a.write_source_tables(db, clobber=True)
try:
    ingest_module = importlib.import_module('ingest.%s' %a.config['input']['ingest_method'].lower())
except ImportError as e:
    raise ImportError('Invalid ingestion method in configuration')
else:
    ingest = getattr(ingest_module, '%sIngest' %a.config['input']['ingest_method'].capitalize())
b = ingest(a.config)
b.load()
print(a.source_id)
b.insert_raw_data(db, a.source_id)
try:
    clump_module = importlib.import_module('clump.%s' %a.config['clump_method'].lower())
except ImportError as e:
    raise ImportError('Invalid clump method in configuration')
else:
    clump = getattr(clump_module, '%sClump' %a.config['clump_method'].capitalize())
b = clump(a.config)
b.clump(db, a.source_id)
try:
    assoc_module = importlib.import_module('assoc.%s' %a.config['assoc_method'].lower())
except ImportError as e:
    raise ImportError('Invalid assoc method in configuration')
else:
    assoc = getattr(assoc_module, '%sAssoc' %a.config['assoc_method'].capitalize())
b = assoc(a.config)
b.assoc(db, a.source_id)

