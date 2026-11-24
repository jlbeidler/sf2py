CREATE SEQUENCE IF NOT EXISTS raw_data_seq START 1;
CREATE SEQUENCE IF NOT EXISTS clump_seq START 1;
CREATE SEQUENCE IF NOT EXISTS fire_seq START 1;
CREATE SEQUENCE IF NOT EXISTS event_seq START 1;
CREATE SEQUENCE IF NOT EXISTS event_day_seq START 1;
CREATE SEQUENCE IF NOT EXISTS reconciliation_stream_seq START 1;
CREATE SEQUENCE IF NOT EXISTS source_seq START 1;
CREATE SEQUENCE IF NOT EXISTS data_attribute_seq START 1;

CREATE TABLE IF NOT EXISTS source (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    name_slug VARCHAR NOT NULL UNIQUE,
    geometry_type VARCHAR NOT NULL,
    ingest_method VARCHAR,
    clump_method VARCHAR NOT NULL,
    assoc_method VARCHAR NOT NULL,
    probability_method VARCHAR NOT NULL,
    fire_type_method VARCHAR NOT NULL,
    granularity VARCHAR NOT NULL,
    fire_name_field VARCHAR,
    new_data_policy VARCHAR NOT NULL,
    latest_data DATE
);

CREATE TABLE IF NOT EXISTS default_weighting (
    id INTEGER PRIMARY KEY,
    detection_rate DOUBLE NOT NULL,
    false_alarm_rate DOUBLE NOT NULL,
    growth_weight DOUBLE NOT NULL,
    location_weight DOUBLE NOT NULL,
    shape_weight DOUBLE NOT NULL,
    size_weight DOUBLE NOT NULL,
    location_uncertainty DOUBLE NOT NULL,
    start_date_uncertainty INTEGER NOT NULL,
    end_date_uncertainty INTEGER NOT NULL,
    name_weight DOUBLE NOT NULL,
    type_weight DOUBLE NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_data (
    id BIGINT PRIMARY KEY,
    area DOUBLE NOT NULL,
    end_date TIMESTAMP NOT NULL,
    shape BLOB NOT NULL,
    start_date TIMESTAMP NOT NULL,
    source_id INTEGER,
    clump_id INTEGER
);

CREATE TABLE IF NOT EXISTS data_attribute (
    id INTEGER PRIMARY KEY DEFAULT nextval('data_attribute_seq'),
    attr_value VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    rawdata_id BIGINT
);

CREATE TABLE IF NOT EXISTS clump (
    id BIGINT PRIMARY KEY,
    area DOUBLE NOT NULL,
    end_date DATE NOT NULL,
    shape BLOB NOT NULL,
    start_date DATE NOT NULL,
    source_id INTEGER NOT NULL,
    fire_id INTEGER
);

CREATE TABLE IF NOT EXISTS fire (
    id INTEGER PRIMARY KEY,
    probability DOUBLE,
    unique_id VARCHAR NOT NULL UNIQUE,
    source_id INTEGER,
    area DOUBLE NOT NULL,
    shape BLOB NOT NULL,
    fire_type VARCHAR NOT NULL,
    fire_name VARCHAR,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS reconciliation_stream (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    reconciliation_method VARCHAR NOT NULL,
    name_slug VARCHAR NOT NULL UNIQUE,
    auto_reconcile BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS event (
    id BIGINT PRIMARY KEY,
    create_date DATE NOT NULL,
    display_name VARCHAR NOT NULL,
    end_date DATE NOT NULL,
    outline_shape BLOB NOT NULL,
    probability DOUBLE NOT NULL,
    start_date DATE NOT NULL,
    total_area DOUBLE NOT NULL,
    unique_id VARCHAR NOT NULL UNIQUE,
    reconciliationstream_id INTEGER,
    fire_type VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS event_fires (
    fire_id INTEGER NOT NULL,
    tmp_event INTEGER,
    event_id BIGINT NOT NULL,
    PRIMARY KEY (fire_id, event_id)
);

CREATE TABLE IF NOT EXISTS event_day (
    id INTEGER PRIMARY KEY,
    daily_area DOUBLE NOT NULL,
    event_date DATE NOT NULL,
    event_id BIGINT NOT NULL,
    clump_id BIGINT,
    location BLOB NOT NULL
);
