import pandas as pd

class EtlFlow:
    def __init__(self, services:dict):
        self._service = services
        
    
    def run(self):
        self._a_flow()
        
        
    def _a_flow(self):
        
        self._service['target'].execute_query('TRUNCATE TABLE A')
        
        # api_data = asyncio.run(self._service['source'].read_api())
        # df = pd.DataFrame(api_data)
        
        # df = self._service['source'].read_excel()
        df = self._service['source'].read_sql('SELECT * FROM B')
        
        df = df.astype(str).where(pd.notnull(df), None)
    
        self._service['target'].insert_data_to_sql(df, 'A')