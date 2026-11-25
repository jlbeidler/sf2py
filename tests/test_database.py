import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from sf2py.database import DataBase


@unittest.skipUnless(importlib.util.find_spec("ibis"), "Ibis is optional")
class DuckDBDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        config_path = Path(self.temp_dir.name) / "duckdb.json"
        config_path.write_text(json.dumps({
            "backend": "duckdb",
            "database": "test.duckdb",
            "epsg": "5070",
        }))
        self.db = DataBase(config_path)
        self.addCleanup(self.db.close)
        self.db.initialize_schema(
            Path(__file__).parents[1]
            / "src" / "sf2py" / "sql" / "duckdb_tables.sql"
        )

    def test_round_trips_geometry_with_its_configured_crs(self):
        self.db.write_dataframe(
            "event_day",
            gpd.GeoDataFrame(
                [{
                    "id": 1,
                    "daily_area": 25.0,
                    "event_date": "2020-01-01",
                    "event_id": 1,
                    "clump_id": -9,
                    "location": Point(-120.0, 40.0),
                }],
                geometry="location",
                crs="EPSG:4326",
            ),
            geometry_columns=("location",),
        )

        result = self.db.read_geodataframe(
            "SELECT * FROM event_day",
            geom_col="location",
            crs="EPSG:4326",
        )

        self.assertEqual(result.crs.to_epsg(), 4326)
        self.assertEqual(result.geometry.iloc[0], Point(-120.0, 40.0))

    def test_sequence_and_dataframe_mapping_update(self):
        self.assertEqual(self.db.next_id("raw_data_seq"), 1)
        self.db.write_dataframe("raw_data", pd.DataFrame([{
            "id": 41,
            "area": 10.0,
            "end_date": "2020-01-01",
            "shape": Point(1.0, 2.0).buffer(1.0).wkb,
            "start_date": "2020-01-01",
            "source_id": 1,
            "clump_id": None,
        }]))

        self.db.update_from_dataframe(
            "raw_data",
            "clumplist",
            pd.DataFrame({"id": [41], "clump_id": [73]}),
            "id",
            "clump_id",
            "clump_id",
        )

        result = self.db.read_sql(
            "SELECT clump_id FROM raw_data WHERE id = 41"
        )
        self.assertEqual(result.iloc[0]["clump_id"], 73)


if __name__ == "__main__":
    unittest.main()
