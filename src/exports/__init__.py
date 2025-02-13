import os.path
import json
from datetime import timedelta, datetime
import pandas as pd
from sqlalchemy import text

class Export():
    '''
    Export the SF2 to a text. Default to BSF style CSV
    '''
    def __init__(self, config, db):
        with open(config) as f:
            self._config = json.load(f)
        atts = ['name','start_date','end_date','export_path','sources']
        for att in atts:
            try:
                setattr(self, att, self._config[att])
            except KeyError as e:
                raise ValueError('Missing %s in config file' %att)
        self.stream_id = self._get_stream_id(db)
        self._get_event_data(db)
        # Add in for retrieving HMS
        self.sources = tuple([x.lower().strip() for x in self.sources])
        self.source_ids = self._get_source_ids(db)
 
    def _get_source_ids(self, db):
        '''
        Get the source IDs of the underlying sources for the stream
        '''
        # Workaround for reconciliation streams with 1 source
        if len(self.sources) == 1:
            self.sourcekey = "('%s')" %self.sources[0]
        else:
            self.sourcekey = self.sources
        q = 'SELECT id, name FROM source WHERE name IN %(sources)s'
        q = text(q % {'sources': self.sourcekey})
        with db.engine.connect() as conn:
            df = pd.read_sql(q, con=conn)
        if len(df) < len(self.sources):
            print(df)
            raise ValueError('Missing sources. Check source names in config')
        elif len(df) > len(self.sources):
            raise ValueError('Duplicate source names in source table')
        if len(df) == 1:
            return "('%s')" %df['id'].values[0]
        else:
            return tuple(df['id'])

    def _get_stream_id(self, db):
        '''
        Get the reconciliation stream ID either from the reconciliation stream table 
        '''
        name_slug = self.name.strip().lower().replace(' ','-')
        with db.engine.connect() as conn:
            q = text("SELECT id FROM reconciliation_stream WHERE name_slug= '%s'" %name_slug)
            df = pd.read_sql(q, con=conn)
            if len(df) == 1:
                stream_id = int(df['id'].values[0])
            else:
                raise ValueError('Somehow there are multiple streams with the same name')
        return stream_id

    def _get_event_data(self, db):
        '''
        Push the raw data to the postgres DB
        '''
        q = """SELECT id, display_name, start_date, end_date, total_area, fire_type FROM \
          event WHERE reconciliationstream_id = %(streamid)s AND \
          (((start_date >= '%(startdate)s') AND (start_date <= '%(enddate)s')) OR \
          ((end_date >= '%(startdate)s') AND (end_date <= '%(enddate)s')))"""
        q = text(q % {'streamid': self.stream_id, 'startdate': self.start_date, 
          'enddate': self.end_date})
        self.events = pd.read_sql(q, con=db.engine.connect())
        self.events.start_date = pd.to_datetime(self.events.start_date)
        self.events.end_date = pd.to_datetime(self.events.end_date)
        self.events['e_txt_id'] = 'SF11E' + self.events['id'].astype(int).astype(str).str.zfill(8)
        self.events.rename(columns={'id': 'event_id', 
          'display_name': 'event_name', 'fire_type': 'type'}, inplace=True)
        self.events['stream_name'] = self.name
        self.events['event_name'] = self.events['event_name'].str.title()

    def _get_event_fires(self, db):
        '''
        Get the event fire xref table
        '''
        event_ids = tuple(self.events['event_id'].drop_duplicates())
        if len(event_ids) == 1:
            event_ids = (event_ids[0], -9)
        q = 'SELECT fire_id, event_id FROM event_fires WHERE event_id IN %(eventids)s'
        df = pd.read_sql(text(q %{'eventids': event_ids}), con=db.engine.connect())
        return df

    def _get_event_days(self, db):
        '''
        Get the daily event date by location
        '''
        event_ids = tuple(self.events['event_id'].drop_duplicates())
        if len(event_ids) == 1:
            event_ids = (event_ids[0], -9)
        q = """SELECT id, daily_area as area, event_date as date_time, event_id, ST_X(location) as longitude, \
          ST_Y(location) as latitude FROM event_day WHERE event_id IN %(eventids)s AND \
          event_date >= '%(startdate)s' AND event_date <= '%(enddate)s'"""
        q = text(q %{'startdate': self.start_date, 'enddate': self.end_date, 
          'eventids': event_ids})
        df = pd.read_sql(q, con=db.engine.connect())
        print(df.head())
        df.date_time = pd.to_datetime(df.date_time)
        df['f_txt_id'] = 'SF11C' + df['id'].astype(int).astype(str).str.zfill(8)
        return df

    def _get_daily_event_shape(self, events, db):
        '''
        Get a geoJSON of the daily event shapes
        '''
        # A record of 1 needs different SQL syntax
        if len(events) == 1:
            events_str = str(events[0])
            wq = f'WHERE id = {events_str}'
        else:
            events_str = ','.join([str(s) for s in events])
            wq = f'WHERE id IN ({events_str})'
        q = """SELECT jsonb_build_object(\
          'type', 'FeatureCollection','features', jsonb_agg(feature))\
          FROM (SELECT jsonb_build_object('type', 'Feature','id', id, 'geometry',\
          ST_AsGeoJSON(ST_Transform(outline_shape, 4326))::jsonb, 'properties', \
          to_jsonb(row) - 'id' - 'outline_shape') AS feature\
          FROM (SELECT * FROM event %(where_events)s) row) features"""
        q = text(q %{'where_events': wq})
        df = pd.read_sql(q, con=db.engine.connect())
        return df.iloc[0]['jsonb_build_object']

    def _get_sources(self, db):
        '''
        Get the sources used in each reconciled event as semicolon concatted list
        '''
        event_fires = self._get_event_fires(db)
        fire_ids = tuple(event_fires['fire_id'].drop_duplicates())        
        if len(fire_ids) == 1:
            fire_ids = (fire_ids[0], -9)
        q = 'SELECT fire.id as fire_id, source.name as sources FROM fire LEFT JOIN source ON \
          source.id = fire.source_id WHERE fire.id IN %(fireids)s'
        fire_sources = pd.read_sql(text(q %{'fireids': fire_ids}), con=db.engine.connect())
        event_fires = event_fires.merge(fire_sources, on='fire_id')
        event_fires.drop_duplicates(['event_id','sources'], inplace=True)
        event_fires = event_fires.groupby('event_id')['sources'].apply(lambda x: ';'.join(x))
        return event_fires.reset_index()

    ACRES_TO_SQM = 4046.8564224
    def _write_events(self, df, fn):
        '''
        Write the BSF-style events file
        '''
        df['id'] = df['e_txt_id']
        # Convert area from sqm to acres
        df['total_area'] = (df['total_area'] / self.ACRES_TO_SQM).round(2)
        cols = ['id','event_name','stream_name','start_date','end_date','total_area','sources']
        df.to_csv(os.path.join(self.export_path, fn), index=False, columns=cols)

    def _write_locations(self, df, fn):
        '''
        Write the BSF-style locations file
        '''
        df['id'] = df['f_txt_id']
        df['event_id'] = df['e_txt_id']
        # Convert area from sqm to acres
        df['area'] = (df['area'] / self.ACRES_TO_SQM).round(2)
        cols = ['id','event_id','event_name','latitude','longitude','date_time','area',
          'type','stream_name','detect_cnt','mean_frp']
        df.to_csv(os.path.join(self.export_path, fn), index=False, columns=cols)

    def _write_shapes(self, daily_shapes, fn):
        '''
        Write the GeoJSON shape files
        '''
        fn = os.path.join(self.export_path, fn)
        with open(fn, 'w') as f:
            json.dump(daily_shapes, f, indent=2) 

    def export(self, db):
        '''
        Default export method to BSF ready format
        '''
        event_days = self._get_event_days(db)
        event_sources = self._get_sources(db)
        sat = self._get_detect_data(db)
        print(self.events[self.events['event_name'] == 'Zink'])
        self.events = self.events.merge(event_sources, on='event_id', how='left')
        print(self.events[self.events['event_id'] == '2082352'])
        for day in list(pd.date_range(self.start_date, self.end_date)):
            day_str = datetime.strftime(day, '%Y%m%d')
            print('Exporting %s' %day_str)
            event_fn = 'events_%s.csv' %day_str
            idx = (self.events['start_date'] <= day) & (self.events['end_date'] >= day)
            day_events = self.events[idx].drop_duplicates('event_id').copy()
            self._write_events(day_events, event_fn)
            day_locs = event_days[event_days['date_time'] == day].copy()
            day_locs = day_locs.merge(day_events, on='event_id', suffixes=['','_ev'])
            day_locs = day_locs.merge(sat, on=['event_id','date_time'], how='left')
            loc_fn = 'fire_locations_%s.csv' %day_str
            self._write_locations(day_locs, loc_fn)
            '''
            if not day_events.empty:
                daily_shapes = self._get_daily_event_shape(list(day_events.event_id), db)
                shp_fn = f'fire_shapes_{day_str}.json'
                self._write_shapes(daily_shapes, shp_fn)
            '''

    def _get_rawsatdata(self, db, srcids):
        '''
        Get the rawdata and clump ids from the raw data table for the selected sources
        '''
        if len(srcids) == 1:
            q = f'SELECT id as rawdata_id, clump_id, start_date FROM raw_data where source_id = {srcids[0]}'
        else:
            q = 'SELECT id as rawdata_id, clump_id, start_date FROM raw_data where source_id IN %(sourceids)s'
            q = text(q %{'sourceids': srcids})
        return pd.read_sql(q, con=db.engine.connect())

    def _get_viirs_frp(self, db, rdids):
        '''
        Get the VIIRS FRP values from the rawdata attribute table
        '''
        # Updated the raw data ID list only for VIIRS
        q = """SELECT rawdata_id FROM data_attribute \
          WHERE name = 'Method' AND attr_value = 'VIIRS' and rawdata_id IN %(rawids)s"""
        q = text(q %{'rawids': rdids})
        df = pd.read_sql(q, con=db.engine.connect())
        # Get the FRP values for VIIRS
        q = """SELECT attr_value as frp, raw_data.clump_id, rawdata_id FROM data_attribute \
          LEFT JOIN raw_data ON data_attribute.rawdata_id = raw_data.id \
          WHERE name = 'FRP' AND rawdata_id IN %(rawids)s"""
        q = text(q %{'rawids': tuple(df.rawdata_id.drop_duplicates())})
        df = pd.read_sql(q, con=db.engine.connect())
        df.frp = df.frp.astype(float)
        return df[df.frp > 0].copy()

    def _get_detect_data(self, db):
        '''
        Export HMS detect count and mean clump VIIRS FRP per daily event ID
        '''
        # Get the source IDs for the sources in this stream that have an hms clump method
        q = """SELECT source.id as source_id FROM source WHERE source.id IN %(sourceids)s AND \
          clump_method = 'hms'"""
        q = text(q %{'sourceids': self.source_ids})
        df = pd.read_sql(q, con=db.engine.connect())
        sat_srcs = tuple(df.source_id.drop_duplicates())
        if len(sat_srcs) == 0:
            print('No satellite sources in stream. Nothing to do.')
            sat = pd.DataFrame(columns=['event_id','detect_cnt','mean_frp','date_time'])
        else:
            rawdata = self._get_rawsatdata(db, sat_srcs)
            detect_cnt = rawdata.groupby(['clump_id','start_date'], as_index=False).count()
            # Get the fire IDs for the clumps
            q = 'SELECT id as clump_id, event_id FROM clump LEFT JOIN event_fires \
              ON clump.fire_id = event_fires.fire_id WHERE clump.id IN %(clumps)s'
            q = text(q %{'clumps': tuple(detect_cnt.clump_id.drop_duplicates())})
            cf_xref = pd.read_sql(q, con=db.engine.connect())
            # Sum up the number of detects per event_day
            detect_cnt = detect_cnt.merge(cf_xref, on='clump_id', how='left')
            idx = ['event_id','start_date']
            detect_cnt = detect_cnt[idx+['rawdata_id',]].groupby(idx, as_index=False).sum()
            # Get the mean clump VIIRS FRP for the fire
            frp = self._get_viirs_frp(db, tuple(rawdata.rawdata_id.drop_duplicates()))
            frp = frp.merge(rawdata[['rawdata_id','start_date']], on='rawdata_id', how='left')
            frp = frp.merge(cf_xref, on='clump_id', how='left')
            frp = frp[idx+['frp',]].groupby(idx, as_index=False).mean()
            # Every fire ID with FRP should have a detect count but every detect count may not have FRP
            sat = detect_cnt.merge(frp, on=idx, how='left')
            sat.frp = sat.frp.round(4)
            sat.start_date = pd.to_datetime(sat.start_date)
            colmap = {'rawdata_id': 'detect_cnt', 'frp': 'mean_frp', 'start_date': 'date_time'}
            sat.rename(columns=colmap, inplace=True)
        return sat

