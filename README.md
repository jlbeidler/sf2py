# Fire Integrated Reconciliation Engine

This repository implements the SmartFire 2 fire activity reconciliation methodology as a Python/PostGIS command-line package. It replaces the earlier client/server and Java Tomcat workflow with command-line ingestion, clumping, association, reconciliation, and CSV export.

The repository is derived from the original GPLv3 SmartFire 2 implementation. See:

- [CFIRE methodology](https://www.tandfonline.com/doi/full/10.1080/10962247.2020.1802365)
- [SmartFire 2 methodology](https://www3.epa.gov/ttnchie1/conference/ei17/session12/raffuse.pdf)

## Processing model

A SmartFire run has two phases:

1. **Import every activity source.** Each source is ingested, converted to point or polygon geometry, clumped into temporary source clumps, and associated into source-level fires.
2. **Reconcile selected sources into a stream.** Fire records are matched in space and time with source-specific uncertainty and weighting values. Reconciled events, daily locations, and daily CSV exports are written to PostGIS and the configured export directory.

```text
source data
  -> ingest (sat / ground / shp)
  -> clump (hms / ics / geomac)
  -> associate (hms / ics / geomac)
  -> reconcile source fires into events
  -> export daily BSF-style CSV and optional GeoJSON
```

## Repository layout

| Path | Purpose |
| --- | --- |
| `src/load_source.py` | Loads one source configuration and processes a source through ingest, clump, and association |
| `src/reconcile_fires.py` | Purges overlapping stream events, reconciles the configured sources, and exports the result |
| `src/purge_stream.py` | Purges events for a reconciliation stream/date range without reconciling or exporting |
| `src/database.py` | Loads PostgreSQL settings and creates the SQLAlchemy engine |
| `src/sources.py` | Writes source definitions and default reconciliation weights |
| `src/ingest/` | CSV, ground-report, satellite, and shapefile ingestion implementations |
| `src/clump/` | HMS, ICS, and perimeter/shapefile clumping implementations |
| `src/assoc/` | HMS, ICS, and perimeter/shapefile association implementations |
| `src/reconcile/` | Reconciliation logic and event/day table writes |
| `src/exports/` | Daily BSF-style event/location export and satellite detection summaries |
| `src/sql/` | PostGIS table, sequence, constraint, and SRID setup |
| `src/config/pg.json` | PostgreSQL connection settings |
| `examples/datasources/` | Example source configurations for HMS, NIFS, ICS-209, and other datasets |
| `examples/streams/` | Example reconciliation stream configurations |
| `examples/inputs/` | Small example input files |
| `data/` | Reference shapefile zip archives |
| `scripts/nrt.csh` | Example near-real-time reconciliation wrapper |
| `scripts/qa/` | Database and GIS QA helper commands |

## Requirements

The implementation requires:

- Linux/Unix shell and Python 3
- PostgreSQL with PostGIS
- Python packages: `SQLAlchemy`, `pandas`, `GeoPandas`, `GeoAlchemy2`, `numpy`, `progress`
- GIS support suitable for reading and writing the source formats (including Esri shapefiles)
- `psql` for database setup

No dependency manifest is included in this repository. Install the Python packages and system PostgreSQL/PostGIS dependencies in your runtime environment before running the scripts.

## Database setup

Create a PostgreSQL database and PostGIS-enabled role. The setup script documents the required database steps:

```bash
psql -U postgres
CREATE DATABASE sf2;
\connect sf2;
CREATE EXTENSION postgis;
CREATE USER sf2 WITH PASSWORD 'replace-with-a-password';
GRANT ALL ON DATABASE sf2 TO sf2;
\q
```

Configure [`src/config/pg.json`](src/config/pg.json) for the target database:

```json
{
  "pgserver": "127.0.0.1",
  "pguser": "sf2",
  "pgpass": "replace-with-a-password",
  "dbname": "sf2",
  "dbport": 5432,
  "epsg": "5070"
}
```

Create the schema from the repository:

```bash
cd src/sql
psql -U sf2 -h 127.0.0.1 sf2 -f smartfire_tables.sql
```

The SQL defaults to EPSG 5070 for source, clump, fire, and event shapes and EPSG 4326 for event-day locations. Change the SRID definitions consistently in [`smartfire_tables.sql`](src/sql/smartfire_tables.sql) and `pg.json` if the study area requires a different projected CRS.

## Prepare activity data

Activity data may be a perimeter shapefile, CSV point data, ground reports, or satellite detections.

Before ingestion:

- Use valid, unique identifiers for fires.
- Check latitude and longitude ranges.
- Verify start and end dates.
- Convert areas to acres for CSV and point sources.
- Correct or simplify geometries.
- Remove invalid areas and duplicate records.
- For perimeter sources, ensure area and fire-type fields are available if required.

The existing data preparation guide recommends:

- **ICS-209:** extract incident tables, normalize dates and incident identifiers, correct short or large-fire durations, and remove duplicates.
- **NIFS/perimeters:** verify dates, calculate acres from geometry where needed, simplify/fix geometries, and remove duplicates.
- **HMS:** retrieve daily detects, combine detections, assign FIPS information, remove duplicates, and split crop, grassland, orchard, or other source types as appropriate.

Example source configs are in [`examples/datasources/`](examples/datasources/). The bundled ZIP archives in [`data/`](data/) contain reference fire type and per-pixel area datasets:

- `FireTypeMap_2020_NA.zip`
- `fccs2_acres_pixel_2020.zip`
- `lf2020_hi_acrespixel.zip`
- `na_forests_acres_pixel_2022.zip`

## Source configuration

Each source config is a JSON file. The HMS example at [`examples/datasources/hms_2020_mwcrops.json`](examples/datasources/hms_2020_mwcrops.json) and the NIFS example at [`examples/datasources/nifs_2020.json`](examples/datasources/nifs_2020.json) show the two common processing patterns.

### Top-level source fields

| Field | Description |
| --- | --- |
| `name` | Unique, descriptive source name. This exact name is used by reconciliation streams. |
| `clump_method` | Clump implementation: `hms`, `ics`, or `geomac`. |
| `assoc_method` | Association implementation: `hms`, `ics`, or `geomac`. |
| `probability_method` | Retain `default` unless extending the implementation. |
| `fire_type_method` | `timeperiod` uses a seasonal shapefile; `field` uses an input fire-type field. |
| `granularity` | Dataset granularity. Leave as `1` in existing configurations. |

### `input`

| Field | Description |
| --- | --- |
| `filename` | Full path to the CSV or shapefile input. |
| `geometry_type` | `point` for point/CSV sources or `polygon` for perimeter sources. |
| `ingest_method` | `sat` for satellite detects, `ground` for ground CSV reports, or `shp` for shapefiles. |
| `new_data_policy` | `replace` replaces the source definition before loading; `append` adds the source data. Annual runs generally use `replace`. |
| `fields` | Maps source columns to reserved fields. |

Reserved input fields:

- `start_date` — required
- `end_date` — optional; if omitted, the start date is used
- `area` — required by ground and perimeter sources
- `fire_id` — unique source fire/incident identifier
- `fire_name` — fire or incident name
- `lat` / `lon` — required for point sources
- `fire_type` — required for `field`; not required for `timeperiod`

### `reconciliation`

These values control matching and attribute ranking:

- `detection_rate` and `false_alarm_rate` describe source detection quality.
- `location_weight`, `size_weight`, `shape_weight`, `growth_weight`, `name_weight`, and `type_weight` rank source attributes. Values are normally fractions from 0 to 1.
- `location_uncertainty` is in kilometers.
- `start_date_uncertainty` and `end_date_uncertainty` are in days.

During reconciliation, larger uncertainty buffers find more potential matches, while larger attribute weights select preferred event fields.

### `clumping` and `association`

- HMS clumping uses `radius` in meters and a pixel threshold. Below the threshold, area is derived from the configured pixel-area shapefile; above it, geospatial area is used.
- Geomac and ICS clumps retain perimeter or ground-record geometry, with source-specific association rules.
- HMS association buffers source fires with `small_fire_distance` or `large_fire_distance` and uses `num_back_days`/`num_forward_days`.
- Geomac association also permits matching records with the same unique `fire_id`.
- ICS association primarily matches records by unique fire identifier within the date window.
- `association.area_calc_method` may be `geom` or `field` for the base association implementation; HMS overrides it with its own pixel-area logic.

## Load a source

Run the source loader from the `src` directory so its relative imports and `config/pg.json` path resolve:

```bash
cd src
python3 load_source.py ../examples/datasources/nifs_2020.json
```

The loader:

1. Creates or replaces the source and default weighting rows.
2. Instantiates `ingest`, `clump`, and `assoc` implementations by config method name.
3. Writes source raw data to `raw_data`.
4. Writes derived rows to `data_attribute`.
5. Clumps the records and writes `clump`.
6. Associates clumps and writes `fire`.
7. Updates raw/clump/fire cross-references.

Repeat this step for every source used by a stream. With `new_data_policy: "replace"`, the source name is used as the replacement key and existing source definitions are clobbered by name slug. Do not reload a reconciled source under the same name unless that replacement is intentional.

## Stream configuration

A stream selects source names and a full date range to reconcile. See [`examples/streams/nei2020_v1.json`](examples/streams/nei2020_v1.json).

```json
{
  "name": "2020NEI v1",
  "start_date": "20191231",
  "end_date": "20210101",
  "sources": [
    "NIFS 2020 v1",
    "HMS 2020 Wildland and Grass v1"
  ],
  "export_path": "/absolute/path/to/exports/2020"
}
```

- `name` must be unique.
- `start_date` and `end_date` use `YYYYMMDD`.
- Include all source names exactly as configured.
- Use the complete date range that will be modeled. A stream should not be reconciled as disjoint pieces.
- `export_path` must exist or be writable. Existing files with generated names may be overwritten.

Regional examples are under [`examples/streams/`](examples/streams/).

## Reconcile and export

```bash
cd src
python3 reconcile_fires.py ../examples/streams/nei2020_v1.json
```

The reconciliation phase:

1. Purges existing events in the stream's date range.
2. Loads all source fires in the stream date range.
3. Applies date and location uncertainty.
4. Combines spatial and temporal matches into events.
5. Writes events and event-fire relationships to PostGIS.
6. Builds daily event records and centroid locations.
7. Exports one event and one location CSV per day.

Generated files use these patterns:

- `events_YYYYMMDD.csv`
- `fire_locations_YYYYMMDD.csv`

The event CSV contains event ID, name, stream name, dates, total area in acres, and contributing source names. The location CSV contains location ID, event ID, coordinates, date, area, fire type, stream name, HMS detection count, and mean HMS/VIIRS FRP where available.

## Purge a stream

To remove reconciled event data for a stream/date range without rerunning reconciliation or export:

```bash
cd src
python3 purge_stream.py ../examples/streams/nei2020_v1.json
```

This deletes matching rows from `event_fires`, `event_day`, and `event` for the stream. Source fires and source-level raw/clump/fire data remain intact, so the stream can be rerun after correction.

## Database and QA notes

The main processed tables are:

| Table | Contents |
| --- | --- |
| `source` | Source definitions |
| `default_weighting` | Source reconciliation weights |
| `raw_data` | Ingested source records and geometries |
| `data_attribute` | Ingested attributes stored by raw-data ID |
| `clump` | Source-level spatial clumps |
| `fire` | Associated fires for a source |
| `reconciliation_stream` | Reconciliation stream definitions |
| `event` | Reconciled cross-source events |
| `event_fires` | Event-to-source-fire relationships |
| `event_day` | Daily event area and location records |

Useful QA commands are provided in [`scripts/qa/`](scripts/qa/):

- [`connect.sh`](scripts/qa/connect.sh) opens a local `psql` session.
- [`extract_info.sh`](scripts/qa/extract_info.sh) exports a sample event and its underlying fires to a Shapefile using `ogr2ogr`.

Update credentials and SQL in these helpers before using them in another environment. The example near-real-time wrapper [`scripts/nrt.csh`](scripts/nrt.csh) shows the reconciliation entry point but references paths/config names that must be adapted to the local environment.

## Troubleshooting

- **Missing source:** confirm the stream name exactly matches `source.name` in the source JSON and that the source was loaded.
- **Duplicate source names:** source names and name slugs must be unique.
- **Missing input fields:** compare the `fields` section to the CSV columns or shapefile attribute names, including case and spacing.
- **PostGIS geometry/SRID errors:** confirm the database, input CRS, and `epsg` setting are consistent.
- **Invalid locations:** inspect longitude/latitude values; the ingest code rejects impossible ranges.
- **Invalid areas:** point and polygon sources discard non-positive areas.
- **Missing export path:** create and verify write access to `export_path`.
- **Stale reconciled events:** purge the stream/date range, correct source or stream configuration, reload sources if needed, and rerun reconciliation.
