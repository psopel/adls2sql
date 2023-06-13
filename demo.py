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
    )
]

res = con.execute_sql(
    mappings=table_mappings,
    sql='SELECT * FROM organizations LIMIT 100;'
)

print(res)