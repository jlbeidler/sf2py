from datetime import datetime, timedelta
import pandas as pd
import geopandas as gpd
from sqlalchemy import text
from . import Clump

class HmsClump(Clump):

    ACRES_TO_SQM = 4046.8564224
    def __init__(self, config):
        super().__init__(config)
        for att in ['radius','pixel_threshold']:
            try:
                val = self._config['clumping'][att]
            except KeyError:
                raise KeyError('Must set clumping %s in config' %att)
            else:
                setattr(self, att, float(val))

    def _set_area(self):
        '''
        Use the pixel threshold from the config. If the pixel/raw input count 
        is <= the threshold then set the area to the sum of the area of the area
        per pixel.
        If the input count is > the threshold then set the area to the geospatial
        area.
        '''
        pixelcount = self.srcmap[['tmp_clump','area']].groupby('tmp_clump').agg(['count','sum']).reset_index()
        pixelcount.columns = ['tmp_clump','count','sum']
        self.clumps = self.clumps.merge(pixelcount, on='tmp_clump')
        self.clumps['area'] = self.clumps['sum'] * self.ACRES_TO_SQM 
        idx = self.clumps['count'] > self.pixel_threshold
        self.clumps.loc[idx, 'area'] = self.clumps.loc[idx, 'shape'].area

