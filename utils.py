from DataLakeConnection import DatalakeConnnection
from TableMapping import TableMapping

import json

def read_table_mappings_from_file(
        storage_account_name: str,
        container_name: str,
        file_name: str,
        sas_token: str
) -> list():
    
    con = DatalakeConnnection(storage_account_name, sas_token)
    json_data = con.get_file_contents(
        container_name=container_name,
        file_path=file_name,
        file_format='JSON'
    )

    mappings = json.loads(json_data)['mappings']

    return[
        TableMapping(
            file_format=x['file_format'],
            cached_table_name=x['cached_table_name'],
            container_name=x['container_name'],
            datalake_path=x['datalake_path']      
        ) for x in mappings
    ]