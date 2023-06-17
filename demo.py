from adls2sql.DataLakeConnection import DatalakeConnnection
from adls2sql.TableMapping import TableMapping
from adls2sql.utils import read_table_mappings_from_file
from datetime import datetime

start_time = datetime.now()

with open('./sastoken.txt','r') as f:
    TOKEN = f.read()

ADLS_ACCOUNT_NAME = 'sopeladlsxxd69'

con = DatalakeConnnection(ADLS_ACCOUNT_NAME,TOKEN)

table_mappings = read_table_mappings_from_file(
        storage_account_name=ADLS_ACCOUNT_NAME,
        container_name='test',
        file_name='mappings.json',
        sas_token=TOKEN
)

print(table_mappings[0].column_mappings)

res = con.execute_sql_dict_output(
    mappings=table_mappings,
    sql='SELECT * FROM twitter'
)

res = con.execute_sql_dict_output(
    mappings=table_mappings,
    sql="""
        SELECT *
        FROM dupa
    """
)

print(res)

end_time = datetime.now()
duration = (end_time-start_time).total_seconds()

print(f'Duration: {duration} seconds.')