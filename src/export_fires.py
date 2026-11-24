#!/usr/bin/env python3

import sys
from database import DataBase
from exports import *

config = sys.argv[1]
database_config = sys.argv[2] if len(sys.argv) > 2 else 'config/pg.json'
db = DataBase(database_config)
a = Export(config, db)
#a.export(db)
#a.export_json(db)
a.export_daily_fire_extents(db)
