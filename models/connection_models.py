

class SapHanaConnectionModel():
    def __init__(self, host, user, password, port = 30015):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        
    @property
    def host(self):
        return self._host
    
    @property
    def port(self):
        return self._port
    
    @property
    def user(self):
        return self._user
    
    @property
    def password(self):
        return self._password


class SQLServerConnectionModel():
    def __init__(self, host, user, password, driver, database):
        self._host = host
        self._driver = driver
        self._user = user
        self._password = password
        self._database = database
        
    @property
    def host(self):
        return self._host
    
    @property
    def driver(self):
        return self._driver
    
    @property
    def user(self):
        return self._user
    
    @property
    def password(self):
        return self._password

    @property
    def database(self):
        return self._database

class ExcelConnectionModel():
    def __init__(self, path, header=None, usecols=None, dtype=None):
        self._path = path
        self._header = header
        self._usecols = usecols
        self._dtype = dtype
        
    @property
    def path(self):
        return self._path
    
    @property
    def header(self):
        return self._header
    
    @property
    def usecols(self):
        return self._usecols
    
    @property
    def dtype(self):
        return self._dtype