from datetime import datetime, timedelta
import pandas as pd
from . import CSVIngest

class GroundIngest(CSVIngest):

    def __init__(self, config):
        super().__init__(config)

    def load(self):
        '''
        Build column list and read into pandas dataframe
        '''
        ground_fields = ('start_date','area','fire_id','fire_name','lat','lon')
        dtype = {}
        remap = {}
        for field in ground_fields:
            try:
                input_col = self._config['input']['fields'][field]
            except KeyError as error:
                raise KeyError('Missing configuration field: %s' %field)
            else:
                if input_col:
                    remap[input_col] = field
                    if field in ['lat','lon','area']:
                        dtype[input_col] = float
                    else:
                        dtype[input_col] = str
                else:
                    raise ValueError('Empty configuration field: %s' %field)
        optional_fields = ('end_date','fire_type')
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
        self.validate_area()
        self.set_points()
        self._validate_locs()
        if self._config['fire_type_method'].lower() == 'timeperiod':
            self.get_firetype_timeperiod()
        self._buffer_points()

    def set_dates(self):
        '''
        Convert the dates to datetime
        
        For ICS209 there is an end date override, but we usually handle this in preprocessing
        fireDays = Days.daysBetween(startDate.toDateMidnight(), reportDate.toDateMidnight()).getDays();
        if((fireDays > 0) && ((area / fireDays) > 10)) {
                endDate = reportDate;
            } else {
                endDate = startDate;
            }
        '''
        self._src['start_date'] = pd.to_datetime(self._src['start_date'])
        if 'end_date' in list(self._src.columns):
            self._src['end_date'] = pd.to_datetime(self._src['end_date'])
            # If end date occurs before start date set the start date to the end date
            idx = (self._src['start_date'] > self._src['end_date'])
            self._src.loc[idx, 'start_date'] = self._src.loc[idx, 'end_date']
            # Use the ICS209 approach as described above
            self._src['daydiff'] = (self._src['end_date'] - self._src['start_date']).dt.days
            idx = ((self._src['daydiff'] ==  0) | (self._src['area']/self._src['daydiff'] <= 10))
            self._src.loc[idx, 'end_date'] = self._src.loc[idx, 'start_date']
        else:
            self._src['end_date'] = self._src['start_date']
        self._src['end_date'] = self._src['end_date'] + timedelta(hours=23, minutes=59, seconds=59)

    def validate_area(self):
        # Drop invalid areas
        idx = (self._src['area'].fillna(0) <= 0)
        if len(self._src[idx]) > 0:
            print('NOTE: Dropping %s records with invalid area' %len(self._src[idx]))
            self._src = self._src[~ idx].copy()

