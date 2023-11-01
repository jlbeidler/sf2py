from math import pi
import pandas as pd
import geopandas as gpd
from geoalchemy2 import Geometry
from sqlalchemy import text

class Ingest():
    '''
    '''
    def __init__(self, config):
        self._config = config
        self._method = self._config['input']['ingest_method']
        # Set default EPSG, will override with DB config value
        self.srid = 5070
        self._filename = self._config['input']['filename']
        self._validate_data_policy()

    ACRES_TO_SQM = 4046.8564224
    def _validate_data_policy(self):
        self._data_policy = self._config['input']['new_data_policy'].lower()
        if self._data_policy not in ('append','replace'):
            raise ValueError('Data policy requires value of append or replace')

    def _get_next_id(self, db):
        with db.engine.connect() as conn:
            result = conn.execute(text("SELECT nextval('raw_data_seq')"))
        return int(result.first()[0])

    def insert_raw_data(self, db, source_id):
        '''
        Push the raw data to the postgres DB
        '''
        self._src.reset_index(inplace=True)
        print('Ingesting %s records from %s to %s' %(len(self._src), 
          self._src['start_date'].sort_values().values[0],
          self._src['end_date'].sort_values(ascending=False).values[0]))
        self._src['source_id'] = source_id
        self._src.rename(columns={'geometry': 'shape'}, inplace=True)
        seq = pd.Series([self._get_next_id(db) for _ in range(len(self._src))])
        self._src['id'] = seq
        self.srid = int(db.srid)
        self._src.set_geometry('shape', inplace=True, crs='EPSG:%s' %self.srid)
        cols = ['id','area','end_date','shape','start_date','source_id']
        dtype = {'shape': Geometry(geometry_type='POLYGON', srid=self.srid)}
        self._src[cols].to_postgis(name='raw_data', con=db.engine, if_exists='append', index=False)#, dtype=dtype)
        srccols = [col for col in list(self._src.columns) if col not in cols]
        if len(srccols) > 1:
            srcatts = self._src[['id',] + srccols].copy()
            for col in srccols:
                srcatts[col] = srcatts[col].fillna('').astype(str)
            srcatts.rename(columns={'id': 'rawdata_id'}, inplace=True)
            srcatts = pd.melt(srcatts, id_vars='rawdata_id', value_vars=srccols, var_name='name', 
              value_name='attr_value')
            srcatts.to_sql(name='data_attribute', con=db.engine, if_exists='append', index=False)

class CSVIngest(Ingest):
    def __init__(self, config):
        super().__init__(config)

    def set_points(self):
        '''
        Define the geometry from lat/lon
        Set the lat/lon CRS to EPSG 4326
        '''
        self._src = gpd.GeoDataFrame(self._src, geometry=gpd.points_from_xy(self._src.lon, self._src.lat))
        self._src = self._src.set_crs(epsg=4326)

    def _buffer_points(self):
        '''
        Create circular shapes around the lat/lon as a centroid. Base on the area
        '''
        print('Buffering activity points to polygons')
        self._src = self._src.to_crs(epsg=self.srid)
        self._src['radius'] = ((self._src['area'] * self.ACRES_TO_SQM)/pi) ** 0.5
        self._src['geometry'] = self._src['geometry'].buffer(self._src.radius, resolution=24)

    def _validate_locs(self):
        '''
        Drop impossible lat/lon
        '''
        idx = ((self._src['lat'] < -90) | (self._src['lat'] > 90) | (self._src['lon'] < -360) | (self._src['lon'] > 360))
        if len(self._src[idx]) > 0:
            print('NOTE: Dropping %s records with invalid lat/lon' %len(self._src[idx]))
            self._src = self._src[~ idx].copy()

    def get_firetype_timeperiod(self):
        '''
        The time period firetype method uses the month and a shapefile to
          determine the type of a fire
        '''
        try:
            shp_fn = self._config['firetype']['shapefile']
        except KeyError as error:
            raise KeyError('Firetype shapefile not set in config')
        else:
            print('Applying shapefile fire types')
            shp = gpd.read_file(shp_fn)
            cols = list(self._src.columns)
            self._src = self._src.to_crs(shp.crs)
            self._src = gpd.sjoin(self._src, shp[['geometry',self._config['firetype']['att']]],
              how='left', op='intersects')
            self._src = self._src.to_crs(epsg=4326)
            self._src.rename(columns={self._config['firetype']['att']: 'fire_months'}, inplace=True)
            self._src = self._src[cols + ['fire_months',]].copy()
            # Default fire type is RX
            self._src['fire_type'] = 'RX'
            idx = (self._src['fire_months'].fillna('') != '')
            self._src.loc[idx, 'fire_type'] = self._src.loc[idx,  
              ['start_date', 'fire_months']].apply(lambda row: self._month_firetype(*row), axis=1)

    def _month_firetype(self, start_date, fire_months):
        '''
        Convert the months to ints and verify against the month from start_date
        Find more pythonic way to do this
        '''
        months = [int(month.strip()) for month in fire_months.split(',') if month.strip() != '']
        if start_date.month in months:
            return 'WF'
        else:
            return 'RX'
'''
'''
