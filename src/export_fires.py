#!/usr/bin/env python3

import sys
from database import DataBase
from exports import *

config = sys.argv[1]
db = DataBase('config/pg.json')
a = Export(config, db)
#a.export(db)
#a.export_json(db)
a.export_daily_fire_extents(db)
