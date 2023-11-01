from datetime import datetime, timedelta
import pandas as pd
import geopandas as gpd
from . import Ingest

class ShpIngest(Ingest):

    def __init__(self, config):
        super().__init__(config)

    def load(self):
        '''
        Build column list and read into pandas dataframe
        '''
        self._src = gpd.read_file(self._filename)
        shp_fields = ('start_date','area','fire_id','fire_name','fire_type')
        remap = {}
        for field in shp_fields:
            try:
                input_col = self._config['input']['fields'][field]
            except KeyError as error:
                raise KeyError('Missing configuration field: %s' %field)
            else:
                if input_col:
                    remap[input_col] = field
                else:
                    raise ValueError('Empty configuration field: %s' %field)
        optional_fields = ('end_date',)
        for field in optional_fields:
            try:
                input_col = self._config['input']['fields'][field]
            except KeyError:
                print('NOTE: Optional Field %s Missing' %field)
            else:
                if input_col:
                    remap[input_col] = field
        self._src.rename(columns=remap, inplace=True)
        # Should be equivalent to rounding to the nearest thousandth
        self._src['geometry'] = self._src['geometry'].simplify(0.001)
        self.set_dates()
        self.validate_area()
        self._src['area'] = self._src['area'] * self.ACRES_TO_SQM
        self._src = self._src.to_crs(epsg=self.srid)

    def set_dates(self):
        '''
        Convert the dates to datetime
        '''
        self._src['start_date'] = pd.to_datetime(self._src['start_date'])
        if 'end_date' in list(self._src.columns):
            self._src['end_date'] = pd.to_datetime(self._src['end_date'])
            # If end date occurs before start date set the start date to the end date
            idx = (self._src['start_date'] > self._src['end_date'])
            self._src.loc[idx, 'start_date'] = self._src.loc[idx, 'end_date']
        else:
            self._src['end_date'] = self._src['start_date']
        self._src['end_date'] = self._src['end_date'] + timedelta(hours=23, minutes=59, seconds=59)

    def validate_area(self):
        # Drop invalid areas
        idx = (self._src['area'].fillna(0) <= 0)
        if len(self._src[idx]) > 0:
            print('NOTE: Dropping %s records with invalid area' %len(self._src[idx]))
            self._src = self._src[~ idx].copy()

