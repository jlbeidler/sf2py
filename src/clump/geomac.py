from progress.bar import Bar
from datetime import datetime, timedelta
import pandas as pd
import geopandas as gpd
from sqlalchemy import text
from . import Clump

class GeomacClump(Clump):

    def __init__(self, config):
        super().__init__(config)
        for att in ['radius','pixel_threshold']:
            try:
                val = self._config['clumping'][att]
            except KeyError:
                raise KeyError('Must set clumping %s in config' %att)
            else:
                setattr(self, att, float(val))

    def clump(self, db, source_id):
        '''
        Clump the raw data for Geomac/Shapefile
        '''
        bar = Bar('Clumping', max=2) 
        with db.engine.connect() as conn:
            self.clumps = gpd.read_postgis(text("SELECT * from raw_data WHERE source_id = '%s'" %source_id),
              con=conn, geom_col='shape')
        bar.next()
        self.clumps.rename(columns={'id': 'rawdata_id'}, inplace=True)
        self._write_clump_data(db)
        bar.next()
        self.srcmap = self.clumps[['id','rawdata_id']].copy()
        self._update_raw_id(db, source_id)
        bar.finish()
