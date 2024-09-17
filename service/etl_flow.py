import pandas as pd

class EtlFlow:
    def __init__(self, services:dict):
        self._service = services
        
    
    def run(self):
        self._a_flow()
        
        
    def _a_flow(self):
        
        self._service['target'].execute_query('TRUNCATE TABLE A')
        
        # df = self._service['source'].read_excel()
        df = self._service['source'].read_sql('SELECT * FROM A')
        
        df = df.astype(str).where(pd.notnull(df), None)
    
        
        insert_query = f"""
            INSERT INTO A(
                A1,
                A2,
                A3,
                A4,
                A5,
                A6
                )
            ) VALUES (?, ?, ?, ?, ?, ?)
            """
            
        self._service['target'].insert_data_to_sql(df, insert_query)