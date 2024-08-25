import geoip2.database

class SerializableReader:
    def __init__(self, db_path, errfile_path):
        self.db_path = db_path
        self.errfile_path = errfile_path
        self.reader = None
        self.errfile = None

    def __getstate__(self):
        return {
            'db_path': self.db_path,
            'errfile_path': self.errfile_path
        }

    def __setstate__(self, state):
        self.db_path = state['db_path']
        self.errfile_path = state['errfile_path']
        self.reader = None
        self.errfile = None

    def get_reader(self):
        if self.reader is None:
            self.reader = geoip2.database.Reader(self.db_path)
        return self.reader

    def get_errfile(self):
        if self.errfile is None:
            self.errfile = open(self.errfile_path, 'a')
        return self.errfile
