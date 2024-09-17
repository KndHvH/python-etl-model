import os
from dotenv import load_dotenv
from service.orms import ExcelService, SapHanaService, SqlServerService
from models.connection_models import ExcelConnectionModel,SapHanaConnectionModel, SQLServerConnectionModel
from service.etl_flow import EtlFlow

load_dotenv()

def main():
    
    excel_service_dex = ExcelService(
        connectionModel=ExcelConnectionModel(
            path=os.getenv("EXCEL_PATH"),
            header=2,
            usecols="B:F",
            dtype=str
        )
    )
    
    sap_hana_service = SapHanaService(
        connectionModel=SapHanaConnectionModel(
            host=os.getenv("SAP_HOST"),
            user=os.getenv("SAP_USER"),
            password=os.getenv("SAP_PASS"),
            port=os.getenv("SAP_PORT")
        )
    )

    sql_server_service = SqlServerService(
        connectionModel=SQLServerConnectionModel(
            host=os.getenv("SQLS_HOST"),
            user=os.getenv("SQLS_USER"),
            password=os.getenv("SQLS_PASS"),
            driver=os.getenv("SQLS_DRIVER"),
            database=os.getenv("SQLS_DB")
        )
    )
    
    etlflow = EtlFlow(services={'source': sap_hana_service, 'target': sql_server_service})
    etlflow.run()

if __name__ == '__main__':
   main()