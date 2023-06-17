from adls2sql.DataLakeConnection import DatalakeConnnection
from adls2sql.TableMapping import TableMapping
from adls2sql.ColumnMapping import ColumnMapping

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
            datalake_path=x['datalake_path'],
            file_format_options=x.get('file_format_options', {}),
            column_mappings=[
                ColumnMapping(
                    ordinal_position=y['ordinal_position'],
                    column_name=y['column_name'],
                    column_type=y['column_type']   
                )
                for y in x.get('columns',[])
            ]
        ) for x in mappings
    ]