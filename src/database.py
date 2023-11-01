import json
from sqlalchemy import create_engine

class DataBase():
    '''
    Database interface
    '''
    def __init__(self, config):
        self.load_config(config)
        self.srid = self._config['epsg']
        self.db = self._config['dbname']
        self._args = (self._config['pguser'], self._config['pgpass'], self._config['pgserver'], 
          self._config['dbport'], self.db)
        self.engine = create_engine('postgresql://%s:%s@%s:%s/%s' %self._args)

    def load_config(self, config):
        with open(config) as f:
            self._config = json.load(f)
