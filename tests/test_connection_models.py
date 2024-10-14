import pytest
from models.connection_models import SapHanaConnectionModel, SQLServerConnectionModel, ExcelConnectionModel

def test_sap_hana_connection_model():
    model = SapHanaConnectionModel('host', 'user', 'pass', 30015)
    assert model.host == 'host'
    assert model.user == 'user'
    assert model.password == 'pass'
    assert model.port == 30015

def test_sql_server_connection_model():
    model = SQLServerConnectionModel('host', 'user', 'pass', 'driver', 'database')
    assert model.host == 'host'
    assert model.user == 'user'
    assert model.password == 'pass'
    assert model.driver == 'driver'
    assert model.database == 'database'

def test_excel_connection_model():
    model = ExcelConnectionModel('path', 2, 'B:F', str)
    assert model.path == 'path'
    assert model.header == 2
    assert model.usecols == 'B:F'
    assert model.dtype == str