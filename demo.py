from DataLakeConnection import DatalakeConnnection
from TableMapping import TableMapping
from utils import read_table_mappings_from_file

with open('./sastoken.txt','r') as f:
    TOKEN = f.read()

ADLS_ACCOUNT_NAME = 'sopeladlsxxd69'

con = DatalakeConnnection(ADLS_ACCOUNT_NAME,TOKEN)
print(con.list_files('test','/'))

table_mappings  = read_table_mappings_from_file(
        storage_account_name=ADLS_ACCOUNT_NAME,
        container_name='test',
        file_name='mappings.json',
        sas_token=TOKEN
)
res = con.execute_sql_dict_output(
    mappings=table_mappings,
    sql='SELECT * FROM twitter'
)

print(res)