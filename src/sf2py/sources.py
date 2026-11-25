#!/usr/bin/env python

'''
Write the input source table
'''

import sys
import json
import pandas as pd

class InsVals():
    def __init__(self, d):
        for k, v in d.items():
            setattr(self, k, v)

class DataSource():
    def __init__(self, config):
        self.load_config(config)
        self._set_atts()

    def load_config(self, config):
        with open(config) as f:
            self.config = json.load(f)
        self.config['name_slug'] = self.config['name'].lower().replace(' ','_')
        print('Loading activity source: %s' %self.config['name'])

    def write_source_tables(self, db, clobber=False):
        '''
        Write the source defs. Optionally, clobber the sources that match the name_slug
        ''' 
        if clobber:
            slug = self.name_slug.replace("'", "''")
            source_ids = db.read_sql(
                f"SELECT id FROM source WHERE name_slug = '{slug}'"
            )
            if not source_ids.empty:
                source_id = int(source_ids.iloc[0]["id"])
                db.execute(f"DELETE FROM default_weighting WHERE id = {source_id}")
                db.execute(f"DELETE FROM source WHERE id = {source_id}")
        self._write_source_table(db)
        self._write_default_weight_table(db)

    def _set_atts(self):
        '''
        Set the relevant source attributes to the object
        '''
        str_cols = ('name','name_slug','clump_method','assoc_method','probability_method',
          'fire_type_method')
        for col in str_cols:
            try:
                val = self.config[col].lower()
            except KeyError:
                raise KeyError('Missing %s in configuration file' %col)
            else:
                setattr(self, col, val)
        for col in ('geometry_type','ingest_method','new_data_policy'):
            try:
                val = self.config['input'][col].lower()
            except KeyError:
                raise KeyError('Missing %s in configuration file' %col)
            else:
                setattr(self, col, val)
        setattr(self, 'granularity', str(self.config['granularity']))
        try:
            val = self.config['input']['fields']['fire_name']
        except KeyError:
            setattr(self, 'fire_name_field', '')
        else:
            setattr(self, 'fire_name_field', val)

    def _write_source_table(self, db):
        '''
        Write the source data to the DB
        '''
        self.source_id = db.next_id("source_seq")
        db.write_dataframe("source", pd.DataFrame([{
            "id": self.source_id,
            "name": self.name,
            "name_slug": self.name_slug,
            "geometry_type": self.geometry_type,
            "ingest_method": self.ingest_method,
            "clump_method": self.clump_method,
            "assoc_method": self.assoc_method,
            "probability_method": self.probability_method,
            "fire_type_method": self.fire_type_method,
            "granularity": self.granularity,
            "fire_name_field": self.fire_name_field,
            "new_data_policy": self.new_data_policy,
        }]))

    def _write_default_weight_table(self, db):
        '''
        Write the default reconciliation weighting to the table for the source
        '''
        vals = InsVals(self.config['reconciliation'])
        db.write_dataframe("default_weighting", pd.DataFrame([{
            "id": self.source_id,
            "detection_rate": vals.detection_rate,
            "false_alarm_rate": vals.false_alarm_rate,
            "growth_weight": vals.growth_weight,
            "location_weight": vals.location_weight,
            "shape_weight": vals.shape_weight,
            "size_weight": vals.size_weight,
            "location_uncertainty": vals.location_uncertainty,
            "start_date_uncertainty": vals.start_date_uncertainty,
            "end_date_uncertainty": vals.end_date_uncertainty,
            "name_weight": vals.name_weight,
            "type_weight": vals.type_weight,
        }]))

