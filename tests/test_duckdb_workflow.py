import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import box

from sf2py.assoc.geomac import GeomacAssoc
from sf2py.clump.geomac import GeomacClump
from sf2py.database import DataBase
from sf2py.exports import Export
from sf2py.ingest import Ingest
from sf2py.reconcile import Reconciliation
from sf2py.sources import DataSource


@unittest.skipUnless(importlib.util.find_spec("ibis"), "Ibis is optional")
class DuckDBWorkflowTests(unittest.TestCase):
    def test_reconciles_exports_and_purges(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            export_path = root / "exports"
            export_path.mkdir()
            db_config = root / "duckdb.json"
            db_config.write_text(json.dumps({
                "backend": "duckdb",
                "database": "sf2.duckdb",
                "epsg": "5070",
            }))
            db = DataBase(db_config)
            try:
                db.initialize_schema(
                    Path(__file__).parents[1]
                    / "src" / "sf2py" / "sql" / "duckdb_tables.sql"
                )
                source_path = root / "source.json"
                source_path.write_text(json.dumps({
                    "name": "Tiny Test",
                    "clump_method": "geomac",
                    "assoc_method": "geomac",
                    "probability_method": "default",
                    "fire_type_method": "field",
                    "granularity": 1,
                    "input": {
                        "filename": "unused",
                        "geometry_type": "polygon",
                        "ingest_method": "shp",
                        "new_data_policy": "append",
                        "fields": {},
                    },
                    "clumping": {"radius": 100, "pixel_threshold": 100},
                    "association": {
                        "num_back_days": 0,
                        "num_forward_days": 0,
                        "area_calc_method": "geom",
                    },
                    "reconciliation": {
                        "detection_rate": 0.9,
                        "false_alarm_rate": 0.1,
                        "growth_weight": 0.5,
                        "location_weight": 0.5,
                        "shape_weight": 0.5,
                        "size_weight": 0.5,
                        "location_uncertainty": 0,
                        "start_date_uncertainty": 0,
                        "end_date_uncertainty": 0,
                        "name_weight": 0.5,
                        "type_weight": 0.5,
                    },
                }))
                source = DataSource(source_path)
                source.write_source_tables(db)

                ingest = Ingest(source.config)
                shape = box(500000, 4100000, 500100, 4100100)
                ingest._src = gpd.GeoDataFrame([{
                    "area": 10000.0,
                    "start_date": pd.Timestamp("2020-01-03"),
                    "end_date": pd.Timestamp("2020-01-03"),
                    "fire_id": "test-fire",
                    "fire_type": "WF",
                    "fire_name": "Test Fire",
                    "geometry": shape,
                }], geometry="geometry", crs="EPSG:5070")
                ingest.insert_raw_data(db, source.source_id)
                GeomacClump(source.config).clump(db, source.source_id)
                GeomacAssoc(source.config).assoc(db, source.source_id)

                stream_path = root / "stream.json"
                stream_path.write_text(json.dumps({
                    "name": "Tiny Stream",
                    "start_date": "2020-01-03",
                    "end_date": "2020-01-03",
                    "sources": ["tiny test"],
                    "export_path": str(export_path),
                }))
                reconciliation = Reconciliation(stream_path, db)
                reconciliation.reconcile(db)

                exporter = Export(stream_path, db)
                exporter.export(db)
                exporter.export_json(db)
                exporter.export_daily_fire_extents(db)

                self.assertEqual(len(db.read_sql("SELECT * FROM raw_data")), 1)
                self.assertEqual(len(db.read_sql("SELECT * FROM data_attribute")), 4)
                self.assertEqual(len(db.read_sql("SELECT * FROM clump")), 1)
                self.assertEqual(len(db.read_sql("SELECT * FROM fire")), 1)
                self.assertEqual(len(db.read_sql("SELECT * FROM event")), 1)
                self.assertEqual(len(db.read_sql("SELECT * FROM event_day")), 1)
                self.assertTrue((export_path / "events_20200103.csv").exists())
                self.assertTrue((export_path / "fire_locations_20200103.csv").exists())
                self.assertTrue((export_path / "fire_events_20200103.json").exists())
                self.assertTrue((export_path / "fire_daily_20200103.json").exists())

                reconciliation.purge_events(db)
                self.assertEqual(len(db.read_sql("SELECT * FROM event")), 0)
                self.assertEqual(len(db.read_sql("SELECT * FROM event_day")), 0)
            finally:
                db.close()


if __name__ == "__main__":
    unittest.main()
