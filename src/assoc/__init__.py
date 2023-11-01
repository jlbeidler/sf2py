from progress.bar import Bar
from math import pi
import uuid
from datetime import timedelta
import pandas as pd
import geopandas as gpd
from sqlalchemy import text

class Association():
    '''
    '''
    def __init__(self, config):
        self._config = config
        self._method = self._config['assoc_method']
        atts = ['num_back_days','num_forward_days']
        for att in atts:
            try:
                setattr(self, att, self._config['association'][att])
            except KeyError as e:
                raise ValueError('Missing %s in config file' %att)

    def _get_next_id(self, db):
        '''
        Get the next sequence value
        '''
        with db.engine.connect() as conn:
            result = conn.execute(text("SELECT nextval('fire_seq')"))
        return result.first()[0]

    def _write_fire_data(self, db):
        '''
        Push the raw data to the postgres DB
        '''
        print('\nAssociated into %s fires' %len(self.fires))
        seq = pd.Series([self._get_next_id(db) for x in range(len(self.fires))])
        self.fires['id'] = seq
        uniq_id = pd.Series([uuid.uuid1().hex for x in range(len(self.fires))])
        self.fires['unique_id'] = uniq_id
        self.fires['probability'] = 1.0 - self._config['reconciliation']['false_alarm_rate']
        cols = ['id','probability','unique_id','source_id','area','shape','fire_type','fire_name',
          'start_date','end_date']
        self.fires.set_geometry('shape', inplace=True, crs='EPSG:%s' %db.srid)
        # Really small buffer to fix geometries
        self.fires['shape'] = self.fires['shape'].buffer(0.00001)
        self.fires[cols].to_postgis(name='fire', con=db.engine, if_exists='append', index=False)

    def _update_clump_id(self, db, source_id):
        '''
        Go back and grab the clump ids to put into the raw id table
        '''
        self.srcmap.rename(columns={'id': 'fire_id'}, inplace=True)
        self.srcmap.rename(columns={'clump_id': 'id'}, inplace=True)
        with db.engine.connect() as conn:
            conn.execute(text("CREATE TEMP TABLE firelist (id INTEGER, fire_id INTEGER) ON COMMIT DROP"))
            self.srcmap[['id','fire_id']].to_sql(name='firelist', con=conn, if_exists='replace', index=False)
            conn.execute(text("UPDATE clump SET fire_id = firelist.fire_id FROM firelist WHERE firelist.id = clump.id"))

    def _write_fire_attributes(self, db):
        ['attr_value','name','fire_id']
        # carry over from data attributes


    def _get_rawdata_id(self, db, source_id):
        '''
        Get the rawdata/clump ID table
        '''
        with db.engine.connect() as conn:
            df = pd.read_sql(text("SELECT id, clump_id FROM raw_data WHERE source_id = '%s'" %source_id), 
              con=conn)
        df.rename(columns={'id': 'rawdata_id'}, inplace=True)
        return df

    def _get_fire_id(self, db, source_id):
        '''
        Get the fire ID attribute from the raw data for each of the clumps
        '''
        rawids = self._get_rawdata_id(db, source_id)
        rawid_list = tuple(rawids['rawdata_id'].drop_duplicates())
        with db.engine.connect() as conn:
            q = """SELECT rawdata_id, attr_value FROM data_attribute WHERE rawdata_id IN %(ids)s
                AND name = 'fire_id'"""
            q = text(q % {'ids': rawid_list})
            fireids = pd.read_sql(q, con=conn)
        fireids = pd.merge(rawids, fireids, on='rawdata_id', how='left')
        fireids = fireids[['clump_id','attr_value']].drop_duplicates('clump_id')
        fireids.rename(columns={'clump_id': 'id', 'attr_value': 'fire_id'}, inplace=True)
        return fireids

    def _set_fire_att(self, att_name, db, source_id):
        '''
        Set the most common fire attribute from the raw data. Used for fire type and fire name.
        '''
        rawids = self._get_rawdata_id(db, source_id)
        rawid_list = tuple(rawids['rawdata_id'].drop_duplicates())
        with db.engine.connect() as conn:
            q = """SELECT rawdata_id, attr_value FROM data_attribute WHERE rawdata_id IN %(ids)s
                AND name = '%(att_name)s'"""
            q = text(q % {'ids': rawid_list, 'att_name': att_name})
            firetype = pd.read_sql(q, con=conn)
        df = pd.merge(self.srcmap[['clump_id','tmp_fire']], rawids, on='clump_id', how='left')
        firetype.rename(columns={'attr_value': att_name}, inplace=True)
        df = pd.merge(df, firetype, on='rawdata_id', how='left')
        df = df[['tmp_fire',att_name,'clump_id']].groupby(['tmp_fire',att_name], 
          as_index=False).agg('count')
        df = df.sort_values('clump_id', ascending=False).drop_duplicates('tmp_fire', keep='first')
        self.fires = pd.merge(self.fires, df[['tmp_fire',att_name]], on='tmp_fire', how='left')

    def _set_fire_dates(self, db, source_id):
        '''
        Set the start and end dates of the fire from the earliest clump start and the latest clump end 
        '''
        with db.engine.connect() as conn:
            df = pd.read_sql(text("SELECT id, start_date, end_date FROM clump WHERE source_id = '%s'" %source_id), con=conn)
            df.rename(columns={'id': 'clump_id'}, inplace=True)
            df = pd.merge(self.srcmap[['clump_id','tmp_fire']], df, on='clump_id', how='left')
        start_dates = df[['tmp_fire','start_date']].sort_values('start_date', ascending=False).drop_duplicates('tmp_fire', keep='last')
        end_dates = df[['tmp_fire','end_date']].sort_values('end_date', ascending=False).drop_duplicates('tmp_fire', keep='first')
        df = pd.merge(start_dates, end_dates, on='tmp_fire', how='outer')
        self.fires = pd.merge(self.fires, df, on='tmp_fire', how='left')

    def _set_area(self, db, source_id):
        '''
        Set the fire area as the area of the union of all the associated clumps
        '''
        self.fires['area'] = self.fires['shape'].area
        self.fires = self.fires[['tmp_fire','shape','area']].copy()

    def _build_fire_table(self, db, source_id):
        '''
        Merge in the fire attributes to build the fire table for output to postgres
        '''
        self.fires.reset_index(inplace=True)
        # Set the fire area    
        self._set_area(db, source_id)
        self.fires['source_id'] = source_id
        # Set the fire types
        self._set_fire_att('fire_type', db, source_id)
        # Set the fire names
        self._set_fire_att('fire_name', db, source_id)
        # Set the start and end dates
        self._set_fire_dates(db, source_id)

    def assoc(self, db, source_id):
        '''
        Associate the clumps in space and time only. No buffering.
        '''
        with db.engine.connect() as conn:
            df = gpd.read_postgis(text("SELECT * from clump WHERE source_id = '%s'" %source_id),
              con=conn, geom_col='shape')
        # Set back and forwards timedeltas
        back_days = timedelta(days=self.num_back_days)
        fwd_days = timedelta(days=self.num_forward_days)
        days = list(set(list(df['start_date'].drop_duplicates()) + \
          list(df['end_date'].drop_duplicates())))
        days.sort()
        df['tmp_fire'] = df.index
        self.fires = gpd.GeoDataFrame()
        self.srcmap = pd.DataFrame()
        bar = Bar('Associating', max=len(days))
        # Iterate over the raw data for the source
        for day in days:
            today = df[(df['start_date'] <= day) & (df['end_date'] >= day)].copy()
            day_match = df[(df['start_date'] >= day - back_days) & (df['end_date'] <= day + fwd_days)]
            today = gpd.overlay(today, day_match, how='intersection')
            today = today[today['tmp_fire_1'].notnull()].copy()
            today.loc[today['id_2'].isnull(), 'id_2'] = \
              today.loc[today['id_2'].isnull(), 'id_1']
            today.sort_values('tmp_fire_1', inplace=True)
            today.drop_duplicates('id_2', keep='first', inplace=True)
            today = today[['id_2','tmp_fire_1']].copy()
            today.rename(columns={'id_2': 'id', 'tmp_fire_1': 'tmp_fire'}, inplace=True)
            df = pd.merge(df, today, on='id', how='left', suffixes=['','_today'])
            # Update all clumps with a tmp_fire ID if it is either newly associated or part of the
            #  underlying association
            tmp_fire_update = df.loc[df['tmp_fire_today'].notnull(), 
              ['tmp_fire','tmp_fire_today']].drop_duplicates('tmp_fire')
            df.drop('tmp_fire_today', axis=1, inplace=True)
            df = pd.merge(df, tmp_fire_update, on='tmp_fire', how='left')
            df.loc[df['tmp_fire_today'].notnull(), 'tmp_fire'] = \
              df.loc[df['tmp_fire_today'].notnull(), 'tmp_fire_today']
            df.drop('tmp_fire_today', axis=1, inplace=True)
            bar.next()
        self.fires = df[['tmp_fire','shape']].dissolve(by='tmp_fire')
        self.srcmap = df[['id','tmp_fire','area']].copy()
        self.srcmap.rename(columns={'id': 'clump_id'}, inplace=True)
        self._build_fire_table(db, source_id)
        self._write_fire_data(db)
        self.srcmap = pd.merge(self.srcmap, self.fires, on='tmp_fire', how='left')
        self._update_clump_id(db, source_id)
        bar.finish()

