from progress.bar import Bar
from datetime import datetime, timedelta
import pandas as pd
import geopandas as gpd
from sqlalchemy import text
from . import Association

class HmsAssoc(Association):

    def __init__(self, config):
        super().__init__(config)
        for att in ['small_fire_distance','large_fire_distance','size_threshold','pixel_threshold']:
            try:
                val = self._config['association'][att]
            except KeyError:
                raise KeyError('Must set association %s in config' %att)
            else:
                setattr(self, att, float(val))

    def _set_area(self, db, source_id):
        '''
        Use the pixel threshold from the config. If the pixel/raw input count 
        is <= the threshold then set the area to the sum of the area of the area
        per pixel.
        If the input count is > the threshold then set the area to the geospatial
        area.
        '''
        pixelcount = self.srcmap[['tmp_fire','area']].groupby('tmp_fire').agg(['count','sum']).reset_index()
        pixelcount.columns = ['tmp_fire','count','sum']
        self.fires = self.fires.merge(pixelcount, on='tmp_fire')
        self.fires['area'] = self.fires['sum']
        idx = self.fires['count'] > self.pixel_threshold
        self.fires.loc[idx, 'area'] = self.fires.loc[idx, 'shape'].area

    def assoc(self, db, source_id):
        '''
        Associate the clumps
        '''
        with db.engine.connect() as conn:
            df = gpd.read_postgis(text("SELECT * from clump WHERE source_id = '%s'" %source_id),
              con=conn, geom_col='shape')
        # Set back and forwards timedeltas
        back_days = timedelta(days=self.num_back_days)
        fwd_days = timedelta(days=self.num_forward_days)
        days = set(list(df['start_date'].drop_duplicates()) + list(df['end_date'].drop_duplicates()))
        df['tmp_fire'] = df.index
        self.fires = gpd.GeoDataFrame()
        self.srcmap = pd.DataFrame()
        bar = Bar('Associating', max=len(days))
        # Iterate over the raw data for the source
        for day in days:
            today = df[(df['start_date'] <= day) & (df['end_date'] >= day)].copy()
            day_match = df[(df['start_date'] >= day - back_days) & (df['end_date'] <= day + fwd_days)]
            # Buffer to the set radius
            today['radius'] = self.small_fire_distance
            today.loc[today['area'] > self.size_threshold, 'radius'] = self.large_fire_distance
            today['shape'] = today.apply(lambda row: row['shape'].buffer(row.radius, resolution=24), axis=1)
            # Intersect today with the buffered days
            # There may be a spatial index speedup here. This is a many to many intersection...needs more research
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
