#!/usr/bin/env python3

import sys
from pathlib import Path

from database import DataBase


if len(sys.argv) != 2:
    raise SystemExit("Usage: python setup_duckdb.py <database-config.json>")

database = DataBase(sys.argv[1])
database.initialize_schema(Path(__file__).parent / "sql" / "duckdb_tables.sql")
database.close()
