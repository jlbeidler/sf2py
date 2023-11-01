from progress.bar import Bar
from datetime import datetime, timedelta
import pandas as pd
import geopandas as gpd
from sqlalchemy import text
from . import Association

class IcsAssoc(Association):

    def __init__(self, config):
        super().__init__(config)

    def assoc(self, db, source_id):
        '''
        Associate the clumps. Ics will associate using unique fire identifier
          within the date range.
        '''
        with db.engine.connect() as conn:
            df = gpd.read_postgis(text("SELECT * from clump WHERE source_id = '%s'" %source_id),
              con=conn, geom_col='shape')
        df.drop('fire_id', axis=1, inplace=True)
        # Get the fire IDs for the clumps
        fire_ids = self._get_fire_id(db, source_id)
        df = pd.merge(df, fire_ids, on='id', how='left')
        df['fire_id'] = df['fire_id'].fillna('').astype(str).str.upper()
        df['tmp_fire'] = -9 
        # Set the date range without the association buffer added
        df['date_range'] = df[['start_date','end_date']].apply(lambda row: \
          set(pd.date_range(row.start_date, row.end_date)), axis=1)
        # Set back and forwards timedeltas
        back_days = timedelta(days=self.num_back_days)
        fwd_days = timedelta(days=self.num_forward_days)
        bar = Bar('Associating', max=len(df))
        # Iterate over the raw data for the source
        for idx in range(len(df)):
            row = df.loc[idx]
            if row['tmp_fire'] < 0:
                clump_id = row['id']
                start_date = row['start_date'] - back_days
                end_date = row['end_date'] + fwd_days
                # Set a date range with the association buffer added
                uncertain_date_range = set(pd.date_range(start_date, end_date))
                fire_id = row['fire_id']
                # Set an index of the data range for the record to select overlapping activity
                # Uses an intersection of sets of date ranges
                df['date_intersect'] = df['date_range'].apply(lambda day: \
                  day & uncertain_date_range)
                date_idx = (df['date_intersect'] != set()) & (df['id'] != clump_id)
                # Select any records that overlap the date range
                date_subset = df[date_idx]
                # Set to a temporary fire ID
                df.loc[df['id'] == clump_id, 'tmp_fire'] = idx
                if len(date_subset) > 0:
                    # Identify any records in the date range with the same unique ID
                    if fire_id.strip() != '':
                        tmp_ids = list(df.loc[(df['fire_id'] == fire_id) & date_idx, 'tmp_fire'].drop_duplicates())
                        df.loc[(df['fire_id'] == fire_id) & date_idx, 'tmp_fire'] = idx
                        # Update any underlying tmp_fire IDs
                        df.loc[(df['fire_id'].isin(tmp_ids)) & (~ df['fire_id'].isin((-9,idx))), 'tmp_fire'] = idx
            bar.next()
        self.fires = df[['tmp_fire','shape']].dissolve(by='tmp_fire')
        self.srcmap = df[['id','tmp_fire','area']].copy()
        self.srcmap.rename(columns={'id': 'clump_id'}, inplace=True)
        self._build_fire_table(db, source_id)
        self._write_fire_data(db)
        self.srcmap = pd.merge(self.srcmap, self.fires, on='tmp_fire', how='left')
        self._update_clump_id(db, source_id)
        bar.finish()
