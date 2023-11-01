
CREATE TABLE clump (
    id bigint NOT NULL,
    area double precision NOT NULL,
    end_date date NOT NULL,
    shape geometry NOT NULL,
    start_date date NOT NULL,
    source_id integer NOT NULL,
    fire_id integer
);

CREATE SEQUENCE clump_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE data_attribute (
    id SERIAL,
    attr_value character varying(100) NOT NULL,
    name character varying(100) NOT NULL,
    rawdata_id bigint
);

CREATE TABLE default_weighting (
    id integer NOT NULL,
    detection_rate double precision NOT NULL,
    false_alarm_rate double precision NOT NULL,
    growth_weight double precision NOT NULL,
    location_weight double precision NOT NULL,
    shape_weight double precision NOT NULL,
    size_weight double precision NOT NULL,
    location_uncertainty double precision DEFAULT 0.0 NOT NULL,
    start_date_uncertainty integer DEFAULT 0 NOT NULL,
    end_date_uncertainty integer DEFAULT 0 NOT NULL,
    name_weight double precision DEFAULT 0.0 NOT NULL,
    type_weight double precision DEFAULT 0.0 NOT NULL
);

CREATE TABLE event (
    id bigint NOT NULL,
    create_date date NOT NULL,
    display_name character varying(100) NOT NULL,
    end_date date NOT NULL,
    outline_shape geometry NOT NULL,
    probability double precision NOT NULL,
    start_date date NOT NULL,
    total_area double precision NOT NULL,
    unique_id character varying(100) NOT NULL,
    reconciliationstream_id integer,
    fire_type character varying(100) NOT NULL
);

CREATE TABLE event_attribute (
    id bigint NOT NULL,
    event_id bigint,
    attr_name character varying(100) NOT NULL,
    attr_value character varying(100) NOT NULL
);

CREATE SEQUENCE event_attribute_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE event_day (
    id integer NOT NULL,
    daily_area double precision NOT NULL,
    event_date date NOT NULL,
    event_id bigint NOT NULL,
    location geometry NOT NULL
);

CREATE SEQUENCE event_day_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE event_fires (
    fire_id integer NOT NULL,
    tmp_event integer,
    event_id bigint NOT NULL
);

CREATE SEQUENCE event_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE fetch_attribute (
    id integer NOT NULL,
    attr_value character varying(100) NOT NULL,
    name character varying(100) NOT NULL,
    fetch_id integer
);

CREATE SEQUENCE fetch_attribute_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE fire (
    id integer NOT NULL,
    probability double precision,
    unique_id character varying(80) NOT NULL,
    source_id integer,
    area double precision NOT NULL,
    shape geometry NOT NULL,
    fire_type character varying(10) NOT NULL,
    fire_name character varying(100),
    start_date date NOT NULL,
    end_date date NOT NULL
);

CREATE TABLE fire_attribute (
    id integer NOT NULL,
    attr_value character varying(100) NOT NULL,
    name character varying(100) NOT NULL,
    fire_id integer
);

CREATE SEQUENCE fire_attribute_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE fire_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE hibernate_sequence
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE job_history (
    name character varying(100) NOT NULL,
    status character varying(100) NOT NULL,
    id integer NOT NULL,
    type character varying(100) NOT NULL,
    start_date timestamp without time zone NOT NULL,
    end_date timestamp without time zone NOT NULL,
    final_status character varying(100) NOT NULL
);

CREATE SEQUENCE job_history_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE raw_data (
    id bigint NOT NULL,
    area double precision NOT NULL,
    end_date timestamp without time zone NOT NULL,
    shape geometry NOT NULL,
    start_date timestamp without time zone NOT NULL,
    source_id integer,
    clump_id integer
);

CREATE SEQUENCE raw_data_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE reconciliation_stream (
    id integer NOT NULL,
    name character varying(100) NOT NULL,
    reconciliation_method character varying(100) NOT NULL,
    name_slug character varying(100) NOT NULL,
    auto_reconcile boolean DEFAULT true NOT NULL
);

CREATE SEQUENCE reconciliation_stream_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE reconciliation_stream_summary_data_layers (
    reconciliation_stream_id integer NOT NULL,
    summary_data_layer_id integer NOT NULL
);

CREATE TABLE reconciliation_weighting (
    id integer NOT NULL,
    detection_rate double precision NOT NULL,
    false_alarm_rate double precision NOT NULL,
    growth_weight double precision NOT NULL,
    location_weight double precision NOT NULL,
    shape_weight double precision NOT NULL,
    size_weight double precision NOT NULL,
    reconciliationstream_id integer,
    source_id integer,
    location_uncertainty double precision DEFAULT 0.0 NOT NULL,
    start_date_uncertainty integer DEFAULT 0 NOT NULL,
    end_date_uncertainty integer DEFAULT 0 NOT NULL,
    name_weight double precision DEFAULT 0.0 NOT NULL,
    type_weight double precision DEFAULT 0.0 NOT NULL
);

CREATE SEQUENCE reconciliation_weighting_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE scheduled_fetch (
    id integer NOT NULL,
    fetch_method character varying(100) NOT NULL,
    last_fetch timestamp without time zone,
    name character varying(100) NOT NULL,
    schedule character varying(100),
    source_id integer,
    date_offset integer NOT NULL
);

CREATE SEQUENCE scheduled_fetch_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE schema_version (
    version character varying(20) NOT NULL,
    description character varying(100),
    type character varying(10) NOT NULL,
    script character varying(200) NOT NULL,
    checksum integer,
    installed_by character varying(30) NOT NULL,
    installed_on timestamp without time zone DEFAULT now(),
    execution_time integer,
    state character varying(15) NOT NULL,
    current_version boolean NOT NULL
);

CREATE TABLE source (
    id integer NOT NULL,
    assoc_method character varying(100) NOT NULL,
    clump_method character varying(100) NOT NULL,
    geometry_type character varying(100) NOT NULL,
    name character varying(100) NOT NULL,
    probability_method character varying(100) NOT NULL,
    name_slug character varying(100) NOT NULL,
    new_data_policy character varying(100) NOT NULL,
    granularity character varying(100) NOT NULL,
    fire_name_field character varying(100),
    latest_data date,
    ingest_method character varying(100),
    fire_type_method character varying(100) NOT NULL
);

CREATE TABLE source_attribute (
    id integer NOT NULL,
    attr_value character varying(100) NOT NULL,
    name character varying(100) NOT NULL,
    source_id integer
);

CREATE SEQUENCE source_attribute_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE source_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE stream_attribute (
    id integer NOT NULL,
    reconciliation_stream_id integer,
    attr_name character varying(100) NOT NULL,
    attr_value character varying(100) NOT NULL
);

CREATE SEQUENCE stream_attribute_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE summary_data_layer (
    id integer NOT NULL,
    data_location character varying(100) NOT NULL,
    end_date date NOT NULL,
    extent geometry NOT NULL,
    name character varying(100) NOT NULL,
    start_date date NOT NULL,
    layer_reading_method character varying(100) NOT NULL,
    name_slug character varying(100) NOT NULL
);

CREATE SEQUENCE summary_data_layer_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE ONLY clump
    ADD CONSTRAINT clump_pkey PRIMARY KEY (id);

ALTER TABLE ONLY data_attribute
    ADD CONSTRAINT data_attribute_pkey PRIMARY KEY (id);

ALTER TABLE ONLY default_weighting
    ADD CONSTRAINT default_weighting_pkey PRIMARY KEY (id);

ALTER TABLE ONLY event_attribute
    ADD CONSTRAINT event_attribute_pkey PRIMARY KEY (id);

ALTER TABLE ONLY event_day
    ADD CONSTRAINT event_day_pkey PRIMARY KEY (id);

ALTER TABLE ONLY event
    ADD CONSTRAINT event_pkey PRIMARY KEY (id);

ALTER TABLE ONLY fetch_attribute
    ADD CONSTRAINT fetch_attribute_pkey PRIMARY KEY (id);

ALTER TABLE ONLY fire_attribute
    ADD CONSTRAINT fire_attribute_pkey PRIMARY KEY (id);

ALTER TABLE ONLY fire
    ADD CONSTRAINT fire_pkey PRIMARY KEY (id);

ALTER TABLE ONLY job_history
    ADD CONSTRAINT job_history_pkey PRIMARY KEY (id);

ALTER TABLE ONLY event_fires
    ADD CONSTRAINT pk_event_fires PRIMARY KEY (fire_id, event_id);

ALTER TABLE ONLY raw_data
    ADD CONSTRAINT raw_data_pkey PRIMARY KEY (id);

ALTER TABLE ONLY reconciliation_stream
    ADD CONSTRAINT reconciliation_stream_pkey PRIMARY KEY (id);

ALTER TABLE ONLY reconciliation_stream_summary_data_layers
    ADD CONSTRAINT reconciliation_stream_summary_data_layers_pkey PRIMARY KEY (reconciliation_stream_id, summary_data_layer_id);

ALTER TABLE ONLY reconciliation_weighting
    ADD CONSTRAINT reconciliation_weighting_pkey PRIMARY KEY (id);

ALTER TABLE ONLY scheduled_fetch
    ADD CONSTRAINT scheduled_fetch_pkey PRIMARY KEY (id);

ALTER TABLE ONLY schema_version
    ADD CONSTRAINT schema_version_primary_key PRIMARY KEY (version);

ALTER TABLE ONLY schema_version
    ADD CONSTRAINT schema_version_script_unique UNIQUE (script);

ALTER TABLE ONLY source_attribute
    ADD CONSTRAINT source_attribute_pkey PRIMARY KEY (id);

ALTER TABLE ONLY source
    ADD CONSTRAINT source_pkey PRIMARY KEY (id);

ALTER TABLE ONLY stream_attribute
    ADD CONSTRAINT stream_attribute_pkey PRIMARY KEY (id);

ALTER TABLE ONLY summary_data_layer
    ADD CONSTRAINT summary_data_layer_pkey PRIMARY KEY (id);

ALTER TABLE ONLY summary_data_layer
    ADD CONSTRAINT unique_layer_name_slug UNIQUE (name_slug);

ALTER TABLE ONLY source
    ADD CONSTRAINT unique_name_slug UNIQUE (name_slug);

ALTER TABLE ONLY reconciliation_stream
    ADD CONSTRAINT unique_stream_name_slug UNIQUE (name_slug);

ALTER TABLE ONLY fire
    ADD CONSTRAINT unique_unique_id UNIQUE (unique_id);

CREATE INDEX idx_clump_by_fire ON clump USING btree (fire_id);

CREATE INDEX idx_clump_by_source ON clump USING btree (source_id);

CREATE INDEX idx_event_attribute ON event_attribute USING btree (event_id);

CREATE INDEX idx_event_by_stream ON event USING btree (reconciliationstream_id);

CREATE INDEX idx_event_fires_by_event ON event_fires USING btree (event_id);

CREATE INDEX idx_event_fires_by_fire ON event_fires USING btree (fire_id);

CREATE UNIQUE INDEX idx_event_unique_id ON event USING btree (unique_id);

CREATE INDEX idx_fire_attribute ON fire_attribute USING btree (fire_id);

CREATE INDEX idx_fire_by_source ON fire USING btree (source_id);

CREATE UNIQUE INDEX idx_fire_unique_id ON fire USING btree (unique_id);

CREATE INDEX idx_raw_data_data_attribute ON data_attribute USING btree (rawdata_id);

CREATE INDEX idx_rawdata_by_clump ON raw_data USING btree (clump_id);

CREATE INDEX idx_rawdata_by_source ON raw_data USING btree (source_id);

CREATE UNIQUE INDEX idx_reconciliation_stream_name_slug ON reconciliation_stream USING btree (name_slug);

CREATE INDEX idx_source_attribute ON source_attribute USING btree (source_id);

CREATE UNIQUE INDEX idx_source_name_slug ON source USING btree (name_slug);

CREATE INDEX idx_stream_attribute ON stream_attribute USING btree (reconciliation_stream_id);

CREATE INDEX idx_weighting_by_source ON reconciliation_weighting USING btree (source_id);

CREATE INDEX idx_weighting_by_stream ON reconciliation_weighting USING btree (reconciliationstream_id);

CREATE INDEX schema_version_current_version_index ON schema_version USING btree (current_version);

ALTER TABLE ONLY fetch_attribute
    ADD CONSTRAINT fetch_attribute_reference FOREIGN KEY (fetch_id) REFERENCES scheduled_fetch(id);

ALTER TABLE ONLY clump
    ADD CONSTRAINT fk1dfcc12340ab7658 FOREIGN KEY (source_id) REFERENCES source(id) ON DELETE CASCADE;

ALTER TABLE ONLY raw_data
    ADD CONSTRAINT fk1dfcc96130ab7658 FOREIGN KEY (source_id) REFERENCES source(id) ON DELETE CASCADE;

ALTER TABLE ONLY event_day
    ADD CONSTRAINT fk1e44877c212ad3c FOREIGN KEY (event_id) REFERENCES event(id);

ALTER TABLE ONLY data_attribute
    ADD CONSTRAINT fk29778847708e8ddc FOREIGN KEY (rawdata_id) REFERENCES raw_data(id) ON DELETE CASCADE;

ALTER TABLE ONLY fire
    ADD CONSTRAINT fk2ff63630ab7658 FOREIGN KEY (source_id) REFERENCES source(id) ON DELETE CASCADE;

ALTER TABLE ONLY scheduled_fetch
    ADD CONSTRAINT fk4e1b34a830ab7658 FOREIGN KEY (source_id) REFERENCES source(id) ON DELETE CASCADE;

ALTER TABLE ONLY event
    ADD CONSTRAINT fk5c6729a22a750f8 FOREIGN KEY (reconciliationstream_id) REFERENCES reconciliation_stream(id);

ALTER TABLE ONLY clump
    ADD CONSTRAINT fk_clump_fire FOREIGN KEY (fire_id) REFERENCES fire(id) MATCH FULL ON DELETE CASCADE;

ALTER TABLE ONLY event_attribute
    ADD CONSTRAINT fk_event_attribute FOREIGN KEY (event_id) REFERENCES event(id) ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE ONLY event_fires
    ADD CONSTRAINT fk_event_fires_event FOREIGN KEY (event_id) REFERENCES event(id);

ALTER TABLE ONLY event_fires
    ADD CONSTRAINT fk_event_fires_fire FOREIGN KEY (fire_id) REFERENCES fire(id);

ALTER TABLE ONLY raw_data
    ADD CONSTRAINT fk_raw_data_clump FOREIGN KEY (clump_id) REFERENCES clump(id) MATCH FULL ON DELETE CASCADE;

ALTER TABLE ONLY stream_attribute
    ADD CONSTRAINT fk_reconciliation_stream_attribute FOREIGN KEY (reconciliation_stream_id) REFERENCES reconciliation_stream(id) ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE ONLY reconciliation_stream_summary_data_layers
    ADD CONSTRAINT fk_stream_layers_layer FOREIGN KEY (summary_data_layer_id) REFERENCES summary_data_layer(id);

ALTER TABLE ONLY reconciliation_stream_summary_data_layers
    ADD CONSTRAINT fk_stream_layers_stream FOREIGN KEY (reconciliation_stream_id) REFERENCES reconciliation_stream(id);

ALTER TABLE ONLY fire_attribute
    ADD CONSTRAINT fka57acfd3ebde83f8 FOREIGN KEY (fire_id) REFERENCES fire(id);

ALTER TABLE ONLY reconciliation_weighting
    ADD CONSTRAINT fkb6c2700e22a750f8 FOREIGN KEY (reconciliationstream_id) REFERENCES reconciliation_stream(id);

ALTER TABLE ONLY reconciliation_weighting
    ADD CONSTRAINT fkb6c2700e30ab7658 FOREIGN KEY (source_id) REFERENCES source(id) ON DELETE CASCADE;

ALTER TABLE ONLY source_attribute
    ADD CONSTRAINT source_attribute_reference FOREIGN KEY (source_id) REFERENCES source(id) ON DELETE CASCADE;

SELECT UpdateGeometrySRID('raw_data','shape',5070);
SELECT UpdateGeometrySRID('clump','shape',5070);
SELECT UpdateGeometrySRID('fire','shape',5070);
SELECT UpdateGeometrySRID('event','outline_shape',5070);
SELECT UpdateGeometrySRID('event_day','location',4326);
