from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from CSVDeserializer import CSVDeserializer
from JSONDeserializer import JSONDeserializer
from AvroDeserializer import AvroDeserializer

import sqlite3
import io

class DatalakeConnnection():

    def __init__(
            self,
            storage_account_name: str,
            sas_token: str,
    ):
        self.service_client = DataLakeServiceClient(
            account_url=f"https://{storage_account_name}.dfs.core.windows.net",
            credential=sas_token
        )
        self.sqlite3_connection = sqlite3.connect(':memory:')

    def list_files(self, container_name: str, path: str) -> list():

        file_system_client = self.service_client.get_file_system_client(
            file_system=container_name
        )

        return([
            x.name for x in file_system_client.get_paths(path=path)
            if not x.is_directory
        ])

    def is_path_a_file(self, path: str) -> bool:
        return('.' in path)

    def get_file_contents(
            self,
            container_name: str,
            file_path: str,
            file_format: str
    ) -> str:
        
        directory_path = '/'.join(
            file_path.split('/')[0:-1]
        ) if len(file_path.split('/')) > 1 else '/'
        file_name = file_path.split('/')[-1]

        file_name = file_path.split('/')[-1]

        file_system_client = self.service_client.get_file_system_client(
            file_system = container_name
        )
        directory_client = file_system_client.get_directory_client(
            directory_path
        )
        file_client = directory_client.get_file_client(file_name)
        download = file_client.download_file()
        if file_format in ('CSV','JSON'):
            return(download.readall().decode('utf-8'))
        elif file_format == 'AVRO':
            res = download.readall()
            return(io.BytesIO(res))
    
    def get_column_names(
            self,
            file_format: str,
            container_name: str,
            file_path: str,
            first_row_as_header: bool
        ) -> list():
        
        file_contents = self.get_file_contents(container_name, file_path, file_format)
        if file_format == 'CSV':
            deserializer = CSVDeserializer(file_contents, first_row_as_header)
        elif file_format == 'JSON':
            deserializer = JSONDeserializer(file_contents)
        elif file_format == 'AVRO':
            deserializer = AvroDeserializer(file_contents)
        return deserializer.get_column_names()
             

    def get_rows(
            self,
            file_format: str,
            container_name: str,
            file_path: str,
            skip_first_row: bool=False
    ) -> list():

        csv_contents = self.get_file_contents(container_name, file_path, file_format)
        headers_in_first_row = skip_first_row
        if file_format == 'CSV':
            deserializer = CSVDeserializer(csv_contents, headers_in_first_row)
        elif file_format =='JSON':
            deserializer = JSONDeserializer(csv_contents)
        elif file_format == 'AVRO':
            deserializer = AvroDeserializer(csv_contents)
        return(deserializer.as_table())

    def get_rows_from_directory(
            self,
            file_format: str,
            container_name: str,
            directory_path: str
        ) -> list:
        """
        Queries all files in a given directory and subdirectories
        and reutrns one combined resultset
        """

        all_files = self.list_files(container_name, directory_path)
        res = []
        for f in all_files:
            rows = self.get_rows(file_format, container_name, f, True)
            res.extend(rows)

        return(res)
    
    
    def get_first_row(
            self,
            container_name: str,
            file_path: str  
        ) -> list():
        """
        Returns first row of a text (csv) file
        as a list of strings
        """

        directory_path = '/'.join(
            file_path.split('/')[0:-1]
        ) if len(file_path.split('/')) > 1 else '/'
        file_name = file_path.split('/')[-1]

        file_system_client = self.service_client.get_file_system_client(
            file_system = container_name
        )
        directory_client = file_system_client.get_directory_client(
            directory_path
        )
        file_client = directory_client.get_file_client(file_name)
        download = file_client.download_file()
        return(download.readall().decode('utf-8').split('\n')[0].split(';'))

    def cache_csv_file_as_table(
        self,
        file_format: str,
        container_name: str,
        file_path: str,
        cached_table_name: str,
        column_mappings: list = []
    ) -> None:

        if column_mappings == []:
            column_names = self.get_column_names(
                file_format,
                container_name,
                file_path,
                True
            )
            column_names = [x.replace(' ','_') for x in column_names]
            column_names = [x.replace('\r','') for x in column_names]
            column_names = [f'"{x}"' for x in column_names]

            column_definitions = [x + ' TEXT' for x in column_names]
            column_definition = ','.join(column_definitions)
        else:
            defs = column_mappings.copy()
            defs.sort(key=lambda x: x.ordinal_position)
            column_names = [f'"{x.column_name}"' for x in defs]
            column_definition=''
            for definition in defs:
                column_definition += f'"{definition.column_name}" {definition.column_type},'
            column_definition = column_definition[:-1]

        ddl_sql = f"""CREATE TABLE IF NOT EXISTS {cached_table_name} (
                {column_definition}
            );
        """
        self.sqlite3_connection.execute(ddl_sql)
        self.sqlite3_connection.commit()

        data_rows = self.get_rows(
            file_format=file_format,
            container_name=container_name,
            file_path=file_path,
            skip_first_row=True
        )      
        i = 0
        y = len(data_rows)
        for x in data_rows:
            i += 1
            dml_sql  = f'INSERT INTO {cached_table_name} ('
            dml_sql += ','.join(column_names)
            dml_sql += ') VALUES (' + ','.join(["'" + str(y).replace("'","''") + "'" for y in x]) + ');'
            self.sqlite3_connection.execute(dml_sql)
        self.sqlite3_connection.commit()

    def cache_csv_dir_as_table(
            self,
            file_format: str,
            container: str,
            path: str,
            cached_table_name: str,
            column_mappings: list = []
        ) -> None:

        if self.is_path_a_file(path):
            self.cache_csv_file_as_table(file_format, container, path, cached_table_name, column_mappings)
        else:
            dir_list = self.list_files(container, path)
            for f in dir_list:
                self.cache_csv_file_as_table(file_format, container, f, cached_table_name, column_mappings)

    def show_cached_table_contents(self, cached_table_name: str) -> None:

        cur = self.sqlite3_connection.cursor()
        cur.execute(f"SELECT * FROM {cached_table_name};")
        return([x for x in cur.fetchall()])

    def execute_sql(
            self,
            mappings: list(),
            sql: str
    ):
        
        self.sqlite3_connection.close()
        self.sqlite3_connection = sqlite3.connect(':memory:')

        for m in mappings:
            self.cache_csv_dir_as_table(m.file_format, m.container_name, m.datalake_path, m.cached_table_name)

        cur = self.sqlite3_connection.cursor()
        cur.execute(sql)
        return([x for x in cur.fetchall()])

    def execute_sql_dict_output(
            self,
            mappings: str,
            sql: str            
        ):

        self.sqlite3_connection.close()
        self.sqlite3_connection = sqlite3.connect(':memory:')

        for m in mappings:
            self.cache_csv_dir_as_table(m.file_format, m.container_name, m.datalake_path, m.cached_table_name, m.column_mappings)

        cur = self.sqlite3_connection.cursor()
        cur.execute(sql)
        names = [description[0] for description in cur.description]
        
        res = []
        for x in cur.fetchall():
            row = {}
            i = -1
            for y in names:
                i += 1
                row[y] = x[i]
            res.append(row)

        return(res)

