from progress.bar import Bar
import uuid
import json
from datetime import timedelta, date
import pandas as pd
import geopandas as gpd
from sqlalchemy import text

class Reconciliation():
    '''
    Spatially and temporally combine the underlying fire activity sources
    '''
    def __init__(self, config, db):
        with open(config) as f:
            self._config = json.load(f)
        atts = ['name','start_date','end_date','sources']
        for att in atts:
            try:
                setattr(self, att, self._config[att])
            except KeyError as e:
                raise ValueError('Missing %s in config file' %att)
        self.stream_id = self._get_stream_id(db)
        self.sources = tuple([x.lower().strip() for x in self.sources])
        # Workaround for reconciliation streams with 1 source
        if len(self.sources) == 1:
            self.sourcekey = "('%s')" %self.sources[0]
        else:
            self.sourcekey = self.sources
        self.source_ids = self._get_source_ids(db)
        self.source_atts = self._get_source_atts(db) 

    def _get_next_id(self, db, seq):
        '''
        Get the next event sequence ID from the DB sequence
        '''
        with db.engine.connect() as conn:
            result = conn.execute(text("SELECT nextval('%s')" %seq))
        return result.first()[0]

    def _get_stream_id(self, db):
        '''
        Get the reconciliation stream ID either from the reconciliation stream table if the 
          stream already exists or from the sequence
        '''
        name_slug = self.name.strip().lower().replace(' ','-')
        with db.engine.connect() as conn:
            q = text("SELECT id FROM reconciliation_stream WHERE name_slug= '%s'" %name_slug)
            df = pd.read_sql(q, con=conn)
            # If the stream does not exist in the table get a fresh stream id number and add it to
            #   the stream table
            if len(df) == 0:
                stream_id = self._get_next_id(db, 'reconciliation_stream_seq')
                row = pd.DataFrame([[stream_id, self.name, '', name_slug, 'f'],], 
                  columns=['id','name','reconciliation_method','name_slug','auto_reconcile']) 
                row.to_sql(name='reconciliation_stream', con=db.engine, if_exists='append', index=False)
            # If it is there then grab the stream ID
            elif len(df) == 1:
                stream_id = int(df['id'].values[0])
            else:
                raise ValueError('Somehow there are multiple streams with the same name')
        return stream_id

    def _get_event_ids(self, db):
        '''
        Get the event IDs from the fire event table for all existing reconciled events
          in the table for this stream for the given date range
        '''
        q = """SELECT id FROM event WHERE reconciliationstream_id = %(streamid)s AND \
          (daterange(start_date, end_date, '[]') && \
          daterange('%(startdate)s', '%(enddate)s', '[]'))"""
        q = text(q % {'streamid': self.stream_id, 'startdate': self.start_date, 
          'enddate': self.end_date})
        df = pd.read_sql(q, con=db.engine.connect()) 
        return tuple(df['id']) 

    def _get_reconciled_fire_ids(self, db, event_ids):
        '''
        Get the fire IDs from the fire event table for all existing reconciled events
          in the table for this stream for the given date range
        '''
        if len(event_ids) > 0:
            q = 'SELECT event_id, fire_id FROM event_fires WHERE event_id IN %(eventids)s'
            df = pd.read_sql(text(q % {'eventids': event_ids}), con=db.engine.connect())
            df.rename(columns={'fire_id': 'id'}, inplace=True)
            return df 
        else:
            return pd.DataFrame() 

    def _get_fires(self, db, fire_ids=()):
        '''
        Load all of the fires associated with the source ID for the given date range
          and that are not in the fire ID list 
        '''
        q = """SELECT id, source_id, area, shape, start_date, end_date FROM fire \
          WHERE source_id IN %(sourceids)s AND \
          (daterange(start_date, end_date, '[]') && \
          daterange('%(startdate)s', '%(enddate)s', '[]'))"""
        if fire_ids: 
            q += ' AND id NOT IN %(fireids)s'
        fire_query = text(q % {'sourceids': self.source_ids, 'startdate': self.start_date, 
          'enddate': self.end_date, 'fireids': fire_ids})
        df = gpd.read_postgis(fire_query, con=db.engine.connect(), geom_col='shape')
        return df

    def _get_reconciled_events(self, db, event_ids):
        '''
        Load all of the existing events with the given event IDs
        '''
        q = 'SELECT id, start_date, end_date, outline_shape FROM event WHERE id IN %(eventids)s' 
        df = gpd.read_postgis(text(q % {'eventids': event_ids}), con=db.engine.connect(), 
          geom_col='outline_shape')
        df.rename(columns={'id': 'event_id', 'outline_shape': 'shape'}, inplace=True)
        return df

    def _get_source_ids(self, db):
        '''
        Get the source IDs of the underlying sources for the stream
        '''
        q = 'SELECT id, name FROM source WHERE name IN %(sources)s'
        q = text(q % {'sources': self.sourcekey})
        with db.engine.connect() as conn:
            df = pd.read_sql(q, con=conn)
        if len(df) < len(self.sources):
            print(df)
            raise ValueError('Missing sources. Check source names in config')
        elif len(df) > len(self.sources):
            raise ValueError('Duplicate source names in source table')
        print('Using sources for reconciliation:\n\t%s' %';'.join(list(df['name'])))
        if len(df) == 1:
            return "('%s')" %df['id'].values[0]
        else:
            return tuple(df['id'])

    def _get_source_atts(self, db):
        '''
        Detection_rate, false_alarm_rate, location_weight, size_weight, shape_weight, growth_weight,
          name_weight, type_weight are all between 0 and 1. Higher weights give higher priority to
          the attribute of the source.
        Location uncertainty is in km. Start and end date uncertainty is in days backwards or forwards. 
        '''
        q = 'SELECT * FROM default_weighting WHERE id IN %(sourceids)s'
        q = text(q % {'sourceids': self.source_ids})
        df = pd.read_sql(q, con=db.engine.connect())
        df.rename(columns={'id': 'source_id'}, inplace=True)
        return df

    def _get_fire_sources(self, db, fire_ids):
        '''
        Get the fire source ids from the fire ids
        '''
        q = 'SELECT id, source_id FROM fire WHERE id IN %(fireids)s'
        q = text(q % {'fireids': fire_ids})
        df = pd.read_sql(q, con=db.engine.connect())
        df.rename(columns={'id': 'fire_id'}, inplace=True)
        return df

    # Mapping of the weighting attributes: {output_field: [input field, weight field], ...}
    weights = {'outline_shape': ['shape','shape_weight'], 'total_area': ['area','size_weight'], 
      'display_name': ['fire_name','name_weight'], 'fire_type': ['fire_type','type_weight']}
    def _get_event_att(self, db, outcol):
        '''
        Set the heighest weighted fire attribute for the event
        '''
        incol = self.weights[outcol][0]
        weightcol = self.weights[outcol][1]
        top_weight = self.srcmap[['tmp_event',weightcol]].sort_values(weightcol,
          ascending=False).drop_duplicates('tmp_event', keep='first')
        event_rep = pd.merge(self.srcmap[['tmp_event','fire_id',weightcol]], top_weight,
          on='tmp_event', how='left', suffixes=['','_top'])
        # Select only the fire IDs that have the top weight for that event
        idx = event_rep[weightcol] == event_rep['%s_top' %weightcol]
        event_rep = event_rep.loc[idx, ['tmp_event','fire_id']].copy()
        fire_ids = tuple(event_rep['fire_id'].drop_duplicates())
        q = 'SELECT id, %(incol)s FROM fire WHERE id IN %(fireids)s'
        q = text(q % {'incol': incol, 'fireids': fire_ids})
        if incol == 'shape':
            df = gpd.read_postgis(q, con=db.engine.connect(), geom_col=incol) 
        else:
            df = pd.read_sql(q, con=db.engine.connect())
        df.rename(columns={'id': 'fire_id', incol: outcol}, inplace=True)
        df = event_rep.merge(df, on='fire_id')
        return df[['tmp_event',outcol]].copy()

    def _set_event_fire_dates(self, db, df):
        '''
        Set the start and end dates of the fire from the earliest fire start and the latest fire end 
        Takes a datafame of tmp_event id and all dates associated with the event. This should come
          from the daily event process post-growth
        '''
        days = df[['tmp_event','date']].sort_values('date', ascending=True)
        start_dates = days.drop_duplicates('tmp_event', keep='first')
        end_dates = days.drop_duplicates('tmp_event', keep='last')
        days = pd.merge(start_dates, end_dates, on='tmp_event', how='outer', 
          suffixes=['_start','_end'])
        days.rename(columns={'date_start': 'start_date', 'date_end': 'end_date'}, inplace=True)
        self.events = self.events.merge(days, on='tmp_event')

    def _set_event_fire_old(self, db):
        '''
        Old method for setting the start and end dates for the fire
        '''
        fire_ids = tuple(self.srcmap['fire_id'])
        q = 'SELECT id, start_date, end_date FROM fire WHERE id IN %(fireids)s'
        df = pd.read_sql(text(q % {'fireids': fire_ids}), con=db.engine.connect())
        df.rename(columns={'id': 'fire_id'}, inplace=True)
        df = pd.merge(self.srcmap[['fire_id','tmp_event']], df, on='fire_id', how='left')
        start_dates = df[['tmp_event','start_date']].sort_values('start_date', 
          ascending=False).drop_duplicates('tmp_event', keep='first')
        end_dates = df[['tmp_event','end_date']].sort_values('end_date', 
          ascending=False).drop_duplicates('tmp_event', keep='last')
        df = pd.merge(start_dates, end_dates, on='tmp_event', how='outer')
        self.events = self.events.merge(df, on='tmp_event')

    def _set_event_area(self, db):
        '''
        Set the event area as the sum of the area of the fires in the highest rated source type
        '''
        df = self._get_event_att(db, 'total_area')
        df = df.groupby('tmp_event', as_index=False).sum()
        self.events = self.events.merge(df, on='tmp_event')

    def _set_event_shape(self, db):
        '''
        Set the event shape as the dissolved area of the highest rate source type
        '''
        df = self._get_event_att(db, 'outline_shape')
        df = gpd.GeoDataFrame(df, geometry='outline_shape')
        df = df.dissolve(by='tmp_event')
        self.events = self.events.merge(df, on='tmp_event')

    def _set_event_probability(self, db):
        '''
        Set the event probability from the inverse of the contributing sources
        '''
        fire_ids = tuple(self.srcmap['fire_id'].drop_duplicates())
        q = 'SELECT id, probability FROM fire WHERE id IN %(fireids)s'
        df = pd.read_sql(text(q % {'fireids': fire_ids}), con=db.engine.connect())
        df.rename(columns={'id': 'fire_id'}, inplace=True)
        df['probability'] = 1 - df['probability']
        df = df.merge(self.srcmap, on='fire_id')
        df = df[['tmp_event','probability']].groupby('tmp_event', as_index=False).prod()
        df['probability'] = 1 - df['probability']
        self.events = self.events.merge(df, on='tmp_event')

    def _set_event_fields(self, db):
        '''
        Merge in the fire attributes to build the fire table for output to postgres
        '''
        # Merge in the source IDs for the fires
        source_ids = self._get_fire_sources(db, tuple(self.srcmap['fire_id']))
        self.srcmap = pd.merge(self.srcmap, source_ids.drop_duplicates('fire_id'), on='fire_id', 
          how='left')
        # And the reconciliation weighting for the sources
        self.srcmap = pd.merge(self.srcmap, self.source_atts, on='source_id', how='left')
        self._set_event_area(db)
        self._set_event_shape(db)
        for att_name in ['display_name','fire_type']:
            att = self._get_event_att(db, att_name)
            att.drop_duplicates('tmp_event', inplace=True)
            self.events = self.events.merge(att, on='tmp_event')
        self._set_event_probability(db)

    def _set_event_days(self, db):
        '''
        Set the area for each of the event days using the proportional area from the source type
          with the highest growth weight. Assign the centroid of the clump polygon to the location.
        At some point in the future it may be useful to carry forward the clump ID or shape so that 
          the polygon can directly into BlueSky. Until that point it can be referenced with the ids.
        '''
        print('Setting event days', flush=True)
        top_weight = self.srcmap[['tmp_event','growth_weight']].sort_values('growth_weight',
          ascending=False).drop_duplicates('tmp_event', keep='first')
        event_rep = pd.merge(self.srcmap[['tmp_event','fire_id','growth_weight']], top_weight,
          on='tmp_event', how='left', suffixes=['','_top'])
        # Select only the fire IDs that have the top growth weight for that event
        idx = event_rep['growth_weight'] == event_rep['growth_weight_top']
        event_rep = event_rep.loc[idx, ['tmp_event','fire_id']].copy()
        fire_ids = tuple(event_rep['fire_id'].drop_duplicates())
        # Retrieve the clumps for the fire source with the highest growth weight in that event
        # Get the location of the clump as the centroid.
        q = '''SELECT id, fire_id, start_date, end_date, area, \
          ST_CENTROID(ST_TRANSFORM(shape,4326)) as location FROM clump \
          WHERE fire_id IN %(fireids)s'''
        df = gpd.read_postgis(text(q % {'fireids': fire_ids}), con=db.engine.connect(), 
          geom_col='location') 
        df.drop_duplicates(['id','fire_id'], inplace=True)
        df.start_date = pd.to_datetime(df.start_date)
        df.end_date = pd.to_datetime(df.end_date)
        # Flatten multi-date clumps so that each day has the same area
        mdclumps = pd.DataFrame(df[(df['start_date'] != df['end_date'])])
        if len(mdclumps) > 0:
            mdclumps['days'] = (mdclumps['end_date'] - mdclumps['start_date']).dt.days + 1
            mdclumps['area'] = mdclumps['area'] / mdclumps['days']
            mdclumps['date'] = mdclumps.apply(lambda row: pd.date_range(row.start_date, 
              row.end_date), axis=1)
            mdclumps = mdclumps.explode('date', ignore_index=True)
            mdclumps['date'] = mdclumps['date'].dt.strftime('%Y-%m-%d')
        else:
            mdclumps['date'] = mdclumps['start_date']
        # Gapfill the clump IDs
        mdclumps['clump_id'] = ('99999' + mdclumps.index.astype(str)).astype(int)
        df = df[df['start_date'] == df['end_date']].copy()
        df.rename(columns={'start_date': 'date', 'id': 'clump_id'}, inplace=True)
        # Concat all of the clumps back together
        df = pd.concat((df, mdclumps))
        df['date'] = pd.to_datetime(df['date'])
        df = event_rep.merge(df, on='fire_id')
        totarea = df[['tmp_event','area']].groupby('tmp_event', as_index=False).sum()
        # Calculate the fraction of the total event area for this source type that occured on the
        #  the given day at that point
        df = pd.merge(df, totarea, on='tmp_event', how='left', suffixes=['','_tot'])
        df['frac'] = df['area']/df['area_tot']
        # Aggregate fractions to a single date and location for an event
        # Cannot aggregate over a geometry data type so set a wkb column, agg and remerge
        df['wkt'] = df['location'].apply(lambda x: x.wkt)
        locs = df[['wkt','location']].drop_duplicates('wkt')
        idx = ['tmp_event','date','wkt','clump_id']
        df = df[idx+['frac',]].groupby(idx, as_index=False).sum()
        df = pd.merge(df, locs, on='wkt', how='left')
        print('Records with clump ID: %s' %len(df[['tmp_event','clump_id','date','location','frac']].drop_duplicates(['tmp_event','clump_id','date','location'])))
        print('Records without clump ID: %s' %len(df[['tmp_event','clump_id','date','location','frac']].drop_duplicates(['tmp_event','date','location'])))
        return df[['tmp_event','clump_id','date','location','frac']].copy()

    def _write_event_data(self, db):
        '''
        Push the raw data to the postgres DB
        '''
        print('Writing events', flush=True)
        seq = pd.Series([self._get_next_id(db, 'event_seq') for x in range(len(self.events))])
        self.events['id'] = seq
        uniq_id = pd.Series([uuid.uuid1().hex for x in range(len(self.events))])
        self.events['unique_id'] = uniq_id
        self.events['reconciliationstream_id'] = self.stream_id
        self.events['create_date'] = date.today()
        self.events['display_name'] = self.events['display_name'].fillna('Unknown Fire')
        self.events = gpd.GeoDataFrame(self.events, geometry='outline_shape')
        # Append the new events
        cols = ['id','create_date','display_name','end_date','outline_shape','probability',
          'start_date','total_area','unique_id','reconciliationstream_id','fire_type']
        self.events[cols].to_postgis(name='event', con=db.engine, if_exists='append', index=False)

    def _write_event_fires(self, db):
        '''
        Write the event fire xref table. Drop records for the event IDs that are reconciled into 
          new events.
        '''
        # Append all of the newly reconciled events 
        cols = ['fire_id','event_id']
        df = pd.merge(self.events[['id','tmp_event']], self.srcmap[['tmp_event','fire_id']], 
          on='tmp_event', how='left')
        df.rename(columns={'id': 'event_id'}, inplace=True)
        df[cols].to_sql(name='event_fires', con=db.engine, if_exists='append', index=False)

    def _write_event_days(self, db, df):
        '''
        Write the daily event date by location
        Takes the set event days output: df[['tmp_event','date','location','frac','clump_id']]
        '''
        events_area = self.events[['tmp_event','id','total_area']].drop_duplicates('tmp_event')
        df = pd.merge(df, events_area, on='tmp_event', how='left')
        df.rename(columns={'id': 'event_id', 'date': 'event_date'}, inplace=True)
        df['daily_area'] = df['frac'] * df['total_area']
        seq = pd.Series([self._get_next_id(db, 'event_day_seq') for x in range(len(df))])
        df['id'] = seq
        df = gpd.GeoDataFrame(df, geometry='location')
        df['clump_id'] = df['clump_id'].fillna(-9).astype(int)
        # Append all of the newly reconciled events 
        cols = ['id','daily_area','event_date','event_id','clump_id','location']
        df[cols].to_postgis(name='event_day', con=db.engine, if_exists='append', index=False)

    def purge_events(self, db, keep_events=''):
        '''
        Wipe out all of the existing events in date range for this stream. Optionally specify
          event IDs to keep
        '''
        print(f'Purging tables of events from stream {self.stream_id}', flush=True)
        # Query the event IDs for the reconciliation stream in this date range
        q = """SELECT id FROM event WHERE reconciliationstream_id = %(streamid)s AND \
          (daterange(start_date, end_date, '[]') && \
          daterange('%(startdate)s', '%(enddate)s', '[]'))"""
        # optionally ignore event IDs in the list
        if keep_events:
            q += ' AND id NOT IN %(eventids)s'
        q = text(q % {'streamid': self.stream_id, 'startdate': self.start_date, 
          'enddate': self.end_date, 'eventids': keep_events})
        df = pd.read_sql(q, con=db.engine)
        event_ids = tuple(df['id'].drop_duplicates())
        if event_ids:
            # Drop the queried event IDs from the 3 event tables
            ef = 'DELETE FROM event_fires WHERE event_id IN %(eventids)s'
            ed = 'DELETE FROM event_day WHERE event_id IN %(eventids)s'
            e = 'DELETE FROM event WHERE id IN %(eventids)s'
            with db.engine.connect() as conn:
                result = conn.execute(text(ef % {'eventids': event_ids}))
                result = conn.execute(text(ed % {'eventids': event_ids}))
                result = conn.execute(text(e % {'eventids': event_ids}))

    def _set_fire_dates(self, start_date, end_date):
        '''
        Generate a date range set
        '''
        return set(pd.date_range(start_date, end_date))

    def reconcile(self, db):
        '''
        Reconcile the underlying source fire data into events
        '''
        event_ids = self._get_event_ids(db)
        if event_ids:
            rec_fires = self._get_reconciled_fire_ids(db, event_ids)
            fire_ids = tuple(rec_fires['id'])
            df = self._get_fires(db, fire_ids)
            recon_events = self._get_reconciled_events(db, event_ids)
            # Append already reconciled events that fall within this time frame to the unreconciled fires
            df = pd.concat((df, recon_events))
        else:
            df = self._get_fires(db)
            df['event_id'] = -9
            rec_fires = pd.DataFrame(columns=['event_id','id'])
        # Bring in the relevant reconciliation parameters for the sources
        cols = ['source_id','start_date_uncertainty','end_date_uncertainty','location_uncertainty']
        df = pd.merge(df, self.source_atts[cols], on='source_id', how='left')
        # Change the uncertainty to a timedelta with a default of 0.
        #  The default should only be applied to existing events.
        df['start_date_uncertainty'] = pd.to_timedelta(df['start_date_uncertainty'].fillna(0), 
          unit='D')
        df['end_date_uncertainty'] = pd.to_timedelta(df['end_date_uncertainty'].fillna(0), 
          unit='D')
        df['location_uncertainty'] = df['location_uncertainty'].fillna(0)
        # Set the date range without the reconciliation buffer added
        df['date_range'] = df[['start_date','end_date']].apply(lambda row: \
          self._set_fire_dates(*row), axis=1)
        # Fill in dummy default values for matching and sorting
        df['tmp_event'] = df.index
        df.loc[df['event_id'] > 0, 'tmp_event'] = -9
        df['event_id'] = df['event_id'].fillna(-9)
        df['id'] = df['id'].fillna(-9)
        # Flag to identify if a fire has already been selected to reconcile, not if it has been
        # Even if a fire isn't reconciled against another fire it becomes part of an event and
        #  doesn't need to be newly reconciled, but can still be reconciled against 
        df['reconciled'] = 0 
        # Set the date range for the loop
        days = list(set(list(df['start_date'].drop_duplicates()) + \
          list(df['end_date'].drop_duplicates())))
        days.sort()
        print('Reconciling %s: %s to %s' %(self.name, self.start_date, self.end_date))
        print('\tReconciling %s fires' %len(df))
        bar = Bar('Reconciling', max=len(days))
        # Iterate over the days in the reconcilation date range to get a mapping of fire IDs to reconcile into events
        for day in days:
            today = df[(df['start_date'] <= day) & (df['end_date'] >= day) & \
              (df['reconciled'] == 0)].copy()
            if not today.empty:
                # Update the "reconciled" flag for the fires going through the process
                df.loc[(df['start_date'] <= day) & (df['end_date'] >= day) & \
                  (df['reconciled'] == 0), 'reconciled'] = 1
                # Set the date ranges with uncertainty for each of the fire records contained
                today['start_date'] = today[['start_date','start_date_uncertainty']].\
                  apply(lambda row: row.start_date - row.start_date_uncertainty, axis=1) 
                today['end_date'] = today[['end_date','end_date_uncertainty']].\
                  apply(lambda row: row.end_date + row.end_date_uncertainty, axis=1) 
                today['date_range'] = today[['start_date','end_date']].apply(lambda row: \
                  self._set_fire_dates(*row), axis=1)
                # Get the widest possible date range for this day
                uncertain_date_range = set(pd.date_range(today.start_date.min(), 
                  today.end_date.max()))
                # Set an index of the data range for the record to select overlapping activity
                # Uses an intersection of sets of date ranges
                df['date_intersect'] = df['date_range'].apply(lambda firedays: \
                  firedays & uncertain_date_range)
                # Select any records that overlap the date range
                day_match = df.loc[df['date_intersect'] != set(), 
                  ['shape','id','date_range','tmp_event']].copy()
                # Convert the uncertainty from km->m and change to a radius
                today['radius'] = today['location_uncertainty'] * 1000 / 2
                # Apply the buffer to the shapes for this day
                today['shape'] = today.apply(lambda row: row['shape'].buffer(row.radius, resolution=24), 
                  axis=1)
                # Spatially overlay the daily dataset
                #today = gpd.overlay(today, day_match, how='intersection')
                today = today.sjoin(day_match, how='left', lsuffix='1', rsuffix='2')
                # Keep spatial intersections that also have date range intersections
                #today = today[today['tmp_event_1'].notnull()].copy()
                today.drop('shape', axis=1, inplace=True)
                today['date_intersect'] = today[['date_range_1','date_range_2']].apply(lambda row: \
                  row.date_range_1 & row.date_range_2, axis=1)
                today = today[today['date_intersect'] != set()].copy()
                # Fill in fire IDs where there is a fire on the day with no spatial intersect
                today.loc[today['id_2'].isnull(), 'id_2'] = \
                  today.loc[today['id_2'].isnull(), 'id_1']
                # Keep newly reconciled events and only one record per fire ID
                today.sort_values('tmp_event_1', inplace=True)
                today.drop_duplicates('id_2', keep='first', inplace=True)
                today = today[['id_2','tmp_event_1']].copy()
                today.rename(columns={'id_2': 'id', 'tmp_event_1': 'tmp_event'}, inplace=True)
                df = pd.merge(df, today, on='id', how='left', suffixes=['','_today'])
                # Update all fires with a tmp_event ID if it is either newly associated or part of the
                #  underlying association
                tmp_event_update = df.loc[df['tmp_event_today'].notnull(),
                  ['tmp_event','tmp_event_today']].drop_duplicates('tmp_event')
                df.drop('tmp_event_today', axis=1, inplace=True)
                df = pd.merge(df, tmp_event_update, on='tmp_event', how='left')
                df.loc[df['tmp_event_today'].notnull(), 'tmp_event'] = \
                  df.loc[df['tmp_event_today'].notnull(), 'tmp_event_today']
                df.drop('tmp_event_today', axis=1, inplace=True)
            bar.next()
        # Set aside reconciled events in time range that are not reconciled in with new fires 
        idx = ((df['event_id'] > 0) & (df['tmp_event'] == -9))
        # Get a list of event IDs to not delete when updating DB tables
        keep_events = tuple(df.loc[idx, 'event_id'].drop_duplicates())
        # Merge in the fire_ids for the fire source that make up any newly reconciled events
        df = df.loc[(df['tmp_event'] > 0), ['event_id','id','tmp_event']].drop_duplicates()
        if df.empty:
            print('\n\tNo new events reconciled for this date range')
        else:
            print('\n\tReconciled into %s new events' %len(df['tmp_event'].drop_duplicates()))
            # Get a list of event IDs to drop when updating fire_events table
            drop_events = tuple(df.loc[df['event_id'].notnull(), 'event_id'].drop_duplicates())
            if len(drop_events) == 1:
                drop_events = (drop_events[0], -9)
            if not rec_fires.empty:
                df = pd.merge(df, rec_fires, on='event_id', how='left', suffixes=['','_event'])
                # Fill in the fire IDs for the merged-in events
                df.loc[df['id'] < 0, 'id'] = df.loc[df['id'] < 0, 'id_event']
            # Define the fire to tmp_event matching
            self.srcmap = df[['id','tmp_event']].copy()
            self.srcmap.rename(columns={'id': 'fire_id'}, inplace=True)
            self.events = pd.DataFrame(df['tmp_event'].drop_duplicates(), columns=['tmp_event',])
            # Get the start and end date, shape, area, name, type, and probability for the new events 
            self._set_event_fields(db)
            event_days = self._set_event_days(db)
            # Set the event fire dates from a dataframe of tmp_event IDs and associated dates
            self._set_event_fire_dates(db, event_days[['tmp_event','date']].drop_duplicates())
            # Remove the existing events that are reconciled into new events
            self.purge_events(db, keep_events)
            # Write the reconciled event table
            self._write_event_data(db) 
            # Write the event_id/fire_id xref table
            self._write_event_fires(db)
            # Write the daily fire event area and locations
            self._write_event_days(db, event_days)
            bar.finish()
