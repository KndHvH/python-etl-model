import pyodbc
import pandas as pd
from hdbcli import dbapi
from fuzzywuzzy import fuzz
from sqlalchemy import create_engine
from models.connection_models import ExcelConnectionModel, SapHanaConnectionModel, SQLServerConnectionModel

class ExcelService():
    def __init__(self, connectionModel:ExcelConnectionModel):
        self._connectionModel = connectionModel
    
    def read_excel(self):
        df = pd.read_excel(self._connectionModel.path, header=self._connectionModel.header, usecols=self._connectionModel.usecols, dtype=self._connectionModel.dtype)   
        self._validate_content(df)
        return df
    
    def _validate_content(self, df):
        if df.empty:
            raise ValueError("DataFrame vazio")
        
        if df.isna().all().any():
            raise ValueError("Uma ou mais colunas contêm apenas dados faltantes.")
        
        if any(df.columns.str.contains('Unnamed')):
            raise ValueError("Os títulos das colunas estão com valores padrão, como 'Unnamed'. Verifique o cabeçalho do Excel.")
        
        for col in df.columns:
            first_row_value = str(df[col].iloc[0])
            
            similarity = fuzz.ratio(col, first_row_value)
            
            if similarity > 70:
                raise ValueError(f"O título da coluna '{col}' é muito semelhante ao dado '{first_row_value}' na primeira linha (similaridade: {similarity}%). Verifique o formato do Excel.")


class SapHanaService():
    def __init__(self, connectionModel:SapHanaConnectionModel):
        self._connectionModel = connectionModel
        


    def _get_connection(self):
        return dbapi.connect(
            address=self._connectionModel.host,
            port=self._connectionModel.port,
            user=self._connectionModel.user,
            password=self._connectionModel.password
        )
    
    
    def execute_query(self, query):
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            if cursor.description:
                return cursor.fetchall()
        except Exception as e:
            print(e)
        finally:
            cursor.close()
            conn.close()
    
    def read_sql(self, query):
        try:
            conn = self._get_connection()
            return pd.read_sql(query, conn)
        except Exception as e:
            print(e)
        finally:
            conn.close()
                
class SqlServerService():
    def __init__(self, connectionModel:SQLServerConnectionModel):
        self._connectionModel = connectionModel


    def _get_connection(self):
        return pyodbc.connect(
            rf'DRIVER={{{self._connectionModel.driver}}};'
            rf'SERVER={self._connectionModel.host};'
            rf'DATABASE={self._connectionModel.database};'
            rf'UID={self._connectionModel.user};'
            rf'PWD={self._connectionModel.password}'
        )
    
    def execute_query(self, query):
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            if cursor.description:
                return cursor.fetchall()
        except Exception as e:
            print("Error:",e)
        finally:
            conn.close()
                
            
    def read_sql(self, query):
        try:
            conn = self._get_connection()
            return pd.read_sql(query, conn)
        except Exception as e:
            print(e)
        finally:
            conn.close()
            
    def insert_data_to_sql(self, df, query):
        conn = self._get_connection()
        cursor = conn.cursor()

        data = [tuple(row) for row in df.to_numpy()]

        try:
            cursor.executemany(query, data)
            conn.commit()
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")
        finally:
            cursor.close()
            conn.close()