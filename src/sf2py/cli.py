"""Command-line entry points for the SmartFire 2 package."""

import argparse
import importlib
from pathlib import Path

from sf2py.database import DataBase
from sf2py.exports import Export
from sf2py.reconcile import Reconciliation
from sf2py.sources import DataSource


def _database_config_argument(parser):
    parser.add_argument(
        "database_config",
        nargs="?",
        default="config/pg.json",
        help="database configuration (default: config/pg.json)",
    )


def load_source():
    parser = argparse.ArgumentParser(description="Load and process an activity source.")
    parser.add_argument("source_config")
    _database_config_argument(parser)
    args = parser.parse_args()

    source = DataSource(args.source_config)
    database = DataBase(args.database_config)
    source.write_source_tables(database, clobber=True)

    methods = (
        ("ingest", source.config["input"]["ingest_method"]),
        ("clump", source.config["clump_method"]),
        ("assoc", source.config["assoc_method"]),
    )
    for package, method in methods:
        try:
            module = importlib.import_module(
                f"sf2py.{package}.{method.lower()}"
            )
        except ImportError as error:
            raise ImportError(f"Invalid {package} method in configuration") from error
        class_name = f"{method.capitalize()}{package.capitalize()}"
        processor = getattr(module, class_name)(source.config)
        if package == "ingest":
            processor.load()
            print(source.source_id)
            processor.insert_raw_data(database, source.source_id)
        elif package == "clump":
            processor.clump(database, source.source_id)
        else:
            processor.assoc(database, source.source_id)


def reconcile():
    parser = argparse.ArgumentParser(
        description="Reconcile configured sources and export daily results."
    )
    parser.add_argument("stream_config")
    _database_config_argument(parser)
    args = parser.parse_args()

    database = DataBase(args.database_config)
    reconciliation = Reconciliation(args.stream_config, database)
    reconciliation.purge_events(database)
    reconciliation.reconcile(database)
    Export(args.stream_config, database).export(database)


def purge_stream():
    parser = argparse.ArgumentParser(
        description="Purge reconciled events for a stream and date range."
    )
    parser.add_argument("stream_config")
    _database_config_argument(parser)
    args = parser.parse_args()

    database = DataBase(args.database_config)
    Reconciliation(args.stream_config, database).purge_events(database)


def export_fires():
    parser = argparse.ArgumentParser(description="Export daily fire extents.")
    parser.add_argument("stream_config")
    _database_config_argument(parser)
    args = parser.parse_args()

    database = DataBase(args.database_config)
    Export(args.stream_config, database).export_daily_fire_extents(database)


def setup_duckdb():
    parser = argparse.ArgumentParser(
        description="Initialize the schema in a DuckDB database."
    )
    parser.add_argument("database_config")
    args = parser.parse_args()

    database = DataBase(args.database_config)
    database.initialize_schema(Path(__file__).parent / "sql" / "duckdb_tables.sql")
    database.close()
