import pytest
from unittest.mock import MagicMock
from service.etl_flow import EtlFlow

@pytest.fixture
def etl_flow():
    mock_source = MagicMock()
    mock_target = MagicMock()
    return EtlFlow(services={'source': mock_source, 'target': mock_target})

def test_run(etl_flow):
    etl_flow.run()
    etl_flow._service['target'].execute_query.assert_called_with('TRUNCATE TABLE A')
    etl_flow._service['source'].read_sql.assert_called_with('SELECT * FROM B')
    etl_flow._service['target'].insert_data_to_sql.assert_called()