import geoip2.database

class SerializableReader:
    def __init__(self, db_path):
        self.db_path = db_path
        self.reader = None

    def __getstate__(self):
        return {
            'db_path': self.db_path,
        }

    def __setstate__(self, state):
        self.db_path = state['db_path']
        self.reader = None

    def get_reader(self):
        if self.reader is None:
            self.reader = geoip2.database.Reader(self.db_path)
        return self.reader
