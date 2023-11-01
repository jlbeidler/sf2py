#!/usr/bin/env python3

import sys
from database import DataBase
from reconcile import *
from exports import *

config = sys.argv[1]
db = DataBase('config/pg.json')
a = Reconciliation(config, db)
a.purge_events(db)
a.reconcile(db)
a = Export(config, db)
a.export(db)
