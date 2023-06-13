from DataLakeConnection import DatalakeConnnection
from TableMapping import TableMapping

with open('./sastoken.txt','r') as f:
    token = f.read()

con = DatalakeConnnection('sopeladlsxxd69',token)
print(con.list_files('test','/'))

table_mappings = [
    TableMapping(
        file_format='CSV',
        cached_table_name='organizations',
        container_name='test',
        datalake_path='organizations-100000.csv'
    ),
    TableMapping(
        file_format='AVRO',
        cached_table_name='twitter',
        container_name='test',
        datalake_path='twitter.avro'
    )
]

res = con.execute_sql_dict_output(
    mappings=table_mappings,
    sql='SELECT * FROM twitter'
)

print(res)