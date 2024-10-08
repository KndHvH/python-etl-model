import time 
import pyodbc
import aiohttp
import pandas as pd
from hdbcli import dbapi
from fuzzywuzzy import fuzz
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from models.connection_models import ExcelConnectionModel, SapHanaConnectionModel, SQLServerConnectionModel, APIConnectionModel

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
            
    def insert_data_to_sql(self, df, table, batch_size=5000):

        placeholders = ', '.join(['?'] * len(df.columns))
        columns = ', '.join(df.columns)
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        conn = self._get_connection()
        cursor = conn.cursor()

        data = [tuple(row) for row in df.to_numpy()]
        total = len(data)
        inicio_tempo = time.time()
        print(f"Left: {total}. Tempo acumulado: {time.time()-inicio_tempo:.2f} segundos")

        try:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                cursor.executemany(query, batch)
                total = total - len(batch)
                print(f"Step {i + batch_size}. Left: {total}  Tempo acumulado: {time.time()-inicio_tempo:.2f} segundos")
                conn.commit()
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")
        finally:
            cursor.close()
            conn.close()
            
            
class APIService():
    def __init__(self, connectionModel:APIConnectionModel):
        self._connectionModel = connectionModel
    
    async def read_api(self) -> pd.DataFrame:
        async with aiohttp.ClientSession() as session:
            payload = await self._fetch_paginated_data(
                session=session, base_url=self._connectionModel.url, ref_date=datetime.today() - timedelta(days=4)
            )

        return payload        
    
    
    async def _fetch_paginated_data(self, session, base_url, ref_date, limit=100, initial_page=0):
        today = datetime.today()
        start_time_total = time.time()
        
        all_items = []

        while ref_date <= today:
            current_page = initial_page
            while True:

                start_time = time.time()

                url = f"{base_url}?dataInicial={ref_date.strftime('%d/%m/%Y')}&inicio={current_page}&limite={limit}"
                print(f"Buscando página {current_page//100} data: {ref_date.strftime('%d/%m')} na URL: {url}")
                
                try:
                    data = await self._fetch_data(session, url)
                except Exception as e:
                    print(f"Erro ao buscar dados: {e}")
                    break

                itens = data.get('objeto', {}).get('itens', [])
                
                if not itens:
                    print(f"Sem mais dados na página {current_page//100}.")
                    break

                all_items.extend(itens)

                end_time = time.time()
                spent_time = end_time - start_time
                print(f"Encontrados {len(itens)} itens na página {current_page//100}. Tempo gasto: {spent_time:.2f} segundos. Tempo gasto total: {end_time-start_time_total:.2f} segundos")
                
                current_page += limit
    
            ref_date += timedelta(days=1)

        print(f"Total de itens encontrados: {len(all_items)}")
        return all_items
    
     
    async def _fetch_data(self, session, url):
        async with session.get(url) as response:
            if response.status != 200:
                raise Exception(f"Erro na requisição: {response.status}")
            return await response.json()