import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely import wkb
from shapely.geometry.base import BaseGeometry
from sqlalchemy import create_engine, text


class DataBase:
    """Database interface for PostgreSQL/PostGIS and Ibis DuckDB backends."""

    def __init__(self, config):
        self._config_path = Path(config).resolve()
        self.load_config(config)
        self.backend = self._config.get("backend", "postgres").lower()
        if self.backend not in ("postgres", "duckdb"):
            raise ValueError(f"Unsupported database backend: {self.backend}")

        self.srid = int(self._config["epsg"])
        self.db = self._config.get("dbname", "")
        self.engine = None
        self.ibis = None

        if self.backend == "postgres":
            args = (
                self._config["pguser"],
                self._config["pgpass"],
                self._config["pgserver"],
                self._config["dbport"],
                self.db,
            )
            self.engine = create_engine(
                "postgresql://%s:%s@%s:%s/%s" % args
            )
        else:
            try:
                import ibis
            except ImportError as error:
                raise ImportError(
                    "The DuckDB backend requires Ibis. Install it with "
                    "`pip install 'ibis-framework[duckdb]'`."
                ) from error

            database = Path(self._config["database"])
            if not database.is_absolute():
                database = self._config_path.parent / database
            database.parent.mkdir(parents=True, exist_ok=True)
            self.db = str(database.resolve())
            self.ibis = ibis.duckdb.connect(database=self.db)

    def load_config(self, config):
        with open(config) as config_file:
            self._config = json.load(config_file)

    @staticmethod
    def _query_string(query):
        return str(query)

    def read_sql(self, query):
        query = self._query_string(query)
        if self.backend == "duckdb":
            return self.ibis.sql(query).execute()
        return pd.read_sql(text(query), con=self.engine)

    def execute(self, query):
        query = self._query_string(query)
        if self.backend == "duckdb":
            return self.ibis.raw_sql(query)
        with self.engine.begin() as connection:
            return connection.execute(text(query))

    def next_id(self, sequence):
        result = self.read_sql(f"SELECT nextval('{sequence}') AS id")
        return int(result.iloc[0]["id"])

    @staticmethod
    def decode_geometry(value):
        if value is None:
            return None
        if isinstance(value, BaseGeometry):
            return value
        raw = getattr(value, "data", value)
        if isinstance(raw, memoryview):
            raw = raw.tobytes()
        if isinstance(raw, (bytes, bytearray)):
            return wkb.loads(bytes(raw))
        raise TypeError(f"Unsupported geometry value: {type(value).__name__}")

    def read_geodataframe(self, query, geom_col, crs=None):
        if self.backend == "postgres":
            return gpd.read_postgis(
                text(self._query_string(query)),
                con=self.engine,
                geom_col=geom_col,
                crs=crs,
            )

        frame = self.read_sql(query)
        frame[geom_col] = frame[geom_col].map(self.decode_geometry)
        return gpd.GeoDataFrame(
            frame, geometry=geom_col, crs=crs or f"EPSG:{self.srid}"
        )

    def write_dataframe(self, name, frame, geometry_columns=()):
        if self.backend == "postgres":
            if geometry_columns:
                geo_frame = gpd.GeoDataFrame(
                    frame.copy(),
                    geometry=geometry_columns[0],
                    crs=getattr(frame, "crs", None) or f"EPSG:{self.srid}",
                )
                geo_frame.to_postgis(
                    name=name, con=self.engine, if_exists="append", index=False
                )
            else:
                frame.to_sql(
                    name=name, con=self.engine, if_exists="append", index=False
                )
            return

        values = frame.drop(columns=list(geometry_columns)).copy()
        for column in geometry_columns:
            values[column] = frame[column].map(
                lambda geometry: (
                    bytes(geometry.wkb)
                    if isinstance(geometry, BaseGeometry)
                    else None
                )
            )
        self.ibis.insert(name, values)

    def update_from_dataframe(
        self, target_table, mapping_table, frame, key_column, value_column,
        target_value_column,
    ):
        columns = f"{key_column} BIGINT, {value_column} BIGINT"
        create = f"CREATE TEMP TABLE {mapping_table} ({columns})"
        update = (
            f"UPDATE {target_table} SET {target_value_column} = mapping.{value_column} "
            f"FROM {mapping_table} AS mapping "
            f"WHERE mapping.{key_column} = {target_table}.{key_column}"
        )
        if self.backend == "postgres":
            with self.engine.begin() as connection:
                connection.execute(text(f"DROP TABLE IF EXISTS {mapping_table}"))
                connection.execute(text(create))
                frame[[key_column, value_column]].to_sql(
                    mapping_table, con=connection, if_exists="append", index=False
                )
                connection.execute(text(update))
        else:
            self.execute(f"DROP TABLE IF EXISTS {mapping_table}")
            self.execute(create)
            self.write_dataframe(
                mapping_table, frame[[key_column, value_column]]
            )
            self.execute(update)
            self.execute(f"DROP TABLE {mapping_table}")

    def initialize_schema(self, schema_file):
        if self.backend != "duckdb":
            raise ValueError("DuckDB schema initialization requires the DuckDB backend")
        schema = Path(schema_file).read_text()
        self.ibis.raw_sql(schema)

    def close(self):
        if self.ibis is not None:
            self.ibis.disconnect()
        if self.engine is not None:
            self.engine.dispose()
