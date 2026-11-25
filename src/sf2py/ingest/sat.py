from datetime import datetime, timedelta
import pandas as pd
import geopandas as gpd
from . import CSVIngest

class SatIngest(CSVIngest):

    def __init__(self, config):
        super().__init__(config)

    DEFAULT_ACRES = 100
    def load(self):
        '''
        Build column list and read into pandas dataframe
        '''
        sat_fields = ['start_date','lat','lon']
        dtype = {}
        remap = {}
        for field in sat_fields:
            try:
                input_col = self._config['input']['fields'][field]
            except KeyError as error:
                raise KeyError('Missing configuration field: %s' %field)
            else:
                if input_col:
                    remap[input_col] = field
                    if field in ['lat','lon']:
                        dtype[input_col] = float
                    else:
                        dtype[input_col] = str
                else:
                    raise ValueError('Empty configuration field: %s' %field)
        optional_fields = ('fire_type','fire_name')
        for field in optional_fields:
            try:
                input_col = self._config['input']['fields'][field]
            except KeyError:
                print('NOTE: Optional Field %s Missing' %field)
            else:
                if input_col:
                    remap[input_col] = field
                    dtype[input_col] = str
        self._src = pd.read_csv(self._filename, dtype=dtype)
        self._src.rename(columns=remap, inplace=True)
        self.set_dates()
        self.set_points()
        self._get_area()
        # Set the fire name for HMS detects that don't have a fire nmae
        self._src['fire_name'] = 'Unknown'
        self._validate_locs()
        if self._config['fire_type_method'].lower() == 'timeperiod':
            self.get_firetype_timeperiod()
        self._buffer_points()
        
    def set_dates(self):
        '''
        Convert the dates to datetime
        '''
        self._src['start_date'] = pd.to_datetime(self._src['start_date'], format='%Y%j')
        self._src['end_date'] = self._src['start_date'] + timedelta(hours=23, minutes=59, seconds=59)

    def _get_area(self):
        '''
        Get the fire area by location
        '''
        try:
            shp_fn = self._config['clumping']['fire_area_shapefile']
        except KeyError as error:
            raise KeyError('Missing fire_area_shapefile path in config')
        else:
            shp = gpd.read_file(shp_fn)
            cols = list(self._src.columns)
            self._src = self._src.to_crs(shp.crs)
            self._src = gpd.sjoin(self._src, shp[['geometry',self._config['clumping']['fire_area_att']]],
              how='left', op='intersects')
            self._src = self._src.to_crs(epsg=4326)
            self._src.rename(columns={self._config['clumping']['fire_area_att']: 'area'}, inplace=True)
            self._src['area'] = self._src['area'].fillna(self.DEFAULT_ACRES)
            self._src = self._src[cols + ['area',]].copy()
