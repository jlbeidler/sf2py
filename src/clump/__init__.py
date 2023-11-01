from progress.bar import Bar
from multiprocessing import Pool
from functools import partial
import pandas as pd
import geopandas as gpd
from numpy import array_split
from sqlalchemy import text
from datetime import datetime

class Clump():
    '''
    '''
    def __init__(self, config):
        self._config = config
        self._method = self._config['clump_method']
        try:
            self.radius = self._config['clumping']['radius']
        except KeyError as e:
            raise ValueError('Missing radius is config file')

    def _get_next_id(self, db):
        with db.engine.connect() as conn:
            result = conn.execute(text("SELECT nextval('clump_seq')"))
        return result.first()[0]

    def _write_clump_data(self, db):
        '''
        Push the raw data to the postgres DB
        '''
        print('\nClumped into %s clumps' %len(self.clumps))
        seq = pd.Series([self._get_next_id(db) for x in range(len(self.clumps))])
        self.clumps['id'] = seq
        cols = ['id','area','end_date','shape','start_date','source_id']
        self.clumps.set_geometry('shape', inplace=True, crs='EPSG:%s' %db.srid)
        self.clumps[cols].to_postgis(name='clump', con=db.engine, if_exists='append', index=False)

    def _update_raw_id(self, db, source_id):
        '''
        Go back and grab the clump ids to put into the raw id table
        '''
        self.srcmap.rename(columns={'id': 'clump_id'}, inplace=True)
        self.srcmap.rename(columns={'rawdata_id': 'id'}, inplace=True)
        with db.engine.connect() as conn:
            conn.execute(text("CREATE TEMP TABLE clumplist (id INTEGER, clump_id INTEGER) ON COMMIT DROP"))
            self.srcmap.to_sql(name='clumplist', con=conn, if_exists='replace', index=False)
            conn.execute(text("UPDATE raw_data SET clump_id = clumplist.clump_id FROM clumplist WHERE clumplist.id = raw_data.id"))

    def _set_area():
        '''
        Set the area of the clump
        The default method is to use the sum of the area fields from the input data. This happens in the .dissolve step.
        '''
        pass

    def _intersect(self, clump, total):
        '''
        Perform a union on a clump of the data
        '''
        ovr = gpd.overlay(clump, total, how='intersection')
#        ovr = clump.sjoin(total, how='left', op='union', lsuffix='1', rsuffix='2')
        return pd.DataFrame(ovr.loc[ovr['id_1'].notnull(), ['id_1','id_2']])

    def clump(self, db, source_id):
        '''
        Default method to clump the raw data by single day in space with a clumping radius.
        Generally used for HMS satellite.
        '''
        with db.engine.connect() as conn:
            df = gpd.read_postgis(text("SELECT * from raw_data WHERE source_id = '%s'" %source_id),
              con=conn, geom_col='shape')
        bar = Bar('Clumping', max=len(df['start_date'].drop_duplicates())) 
        self.srcmap = pd.DataFrame()
        for day in list(df['start_date'].drop_duplicates()):
            today = df[df['start_date'] == day].copy()
            # print('\n', len(today), day, datetime.now()) # debug
            # Buffer the shapes by the configured radius
            today['shape'] = today['shape'].buffer(self.radius, resolution=8)
            ovr_res = []
            clump_intersect = partial(self._intersect, total=today[['shape','id']])
            # Set the number of processes
            n_proc = 4
            if len(today) < n_proc:
                n_proc = len(today)
            with Pool(n_proc) as pool:
                for res in pool.map(clump_intersect, array_split(today[['shape','id']], n_proc)):
                    ovr_res.append(res)
            pool.close()
            # print(day, datetime.now()) # debug
            today_ovr = pd.concat(ovr_res).drop_duplicates()
            today['tmp_clump'] = -9
            # Iterate over the fire IDs for the day
            for fid in list(today['id']):
                # Find all intersecting fire IDs
                intersects = list(today_ovr.loc[today_ovr['id_1'] == fid, 'id_2'].drop_duplicates())
                # Update the tmp_clumps for any underlying fires already clumped
                tmp_clumps = list(today.loc[today['id'].isin(intersects), 'tmp_clump'])
                idx = (today['tmp_clump'].isin(tmp_clumps)) & (today['tmp_clump'] != -9)
                today.loc[idx, 'tmp_clump'] = fid
                # Then update the tmp_clumps for the intersecting fires
                today.loc[today['id'].isin(intersects), 'tmp_clump'] = fid
            self.srcmap = pd.concat((self.srcmap, today[['tmp_clump','id','area']]))
            bar.next()
        cols = ['id','start_date','end_date','shape']
        self.clumps = df[cols].merge(self.srcmap[['id','tmp_clump']], on='id', how='left')
        # Buffer the shapes by the configured radius
        self.clumps['shape'] = self.clumps['shape'].buffer(self.radius, resolution=24)
        self.srcmap.rename(columns={'id': 'rawdata_id'}, inplace=True)
        self.clumps = gpd.GeoDataFrame(self.clumps, 
          geometry='shape').dissolve(by='tmp_clump')
        self.clumps['source_id'] = source_id
        self._set_area()
        self._write_clump_data(db)
        self.srcmap = pd.merge(self.srcmap[['rawdata_id','tmp_clump']], 
          self.clumps[['id','tmp_clump']], 
          on='tmp_clump', how='left')
        self._update_raw_id(db, source_id)
        bar.finish()
