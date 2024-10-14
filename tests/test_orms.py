import pytest
import pandas as pd
import os
from service.orms import ExcelService
from models.connection_models import ExcelConnectionModel

@pytest.fixture
def excel_service():
    data = {
        'A': ['valor1', 'valor2', 'valor3'],
        'B': ['valor1', 'valor2', 'valor3'],
        'C': ['valor4', 'valor5', 'valor6'],
        'D': ['valor7', 'valor8', 'valor9'],
        'E': ['valor10', 'valor11', 'valor12'],
        'F': ['valor13', 'valor14', 'valor15'],
        'G': ['valor16', 'valor17', 'valor18']
    }
    df = pd.DataFrame(data)
    
    temp_excel_path = 'temp_test.xlsx'
    df.to_excel(temp_excel_path, index=False, header=True)
    
    connection_model = ExcelConnectionModel(
        path=temp_excel_path,
        header=0,
        usecols='B:F',
        dtype=str
    )
    service = ExcelService(connection_model)

    yield service

    os.remove(temp_excel_path)


def test_read_excel(excel_service):
    df = excel_service.read_excel()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    
def test_read_excel_and_self_validate(excel_service):
    df = excel_service.read_excel()
    excel_service._validate_content(df)

def test_validate_content_empty(excel_service):
    df = pd.DataFrame()
    with pytest.raises(ValueError):
        excel_service._validate_content(df)

def test_validate_content_missing_data(excel_service):
    df = pd.DataFrame({'A': [None, None]})
    with pytest.raises(ValueError):
        excel_service._validate_content(df)

def test_validate_content_unnamed(excel_service):
    df = pd.DataFrame({'Unnamed: 0': [1, 2]})
    with pytest.raises(ValueError):
        excel_service._validate_content(df)

def test_validate_content_similarity(excel_service):
    df = pd.DataFrame({'Column': ['Column']})
    with pytest.raises(ValueError):
        excel_service._validate_content(df)
