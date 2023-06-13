class TableMapping(object):

    def __init__(
            self,
            file_format: str,
            cached_table_name: str, 
            container_name: str,
            datalake_path: str,
            column_mappings: list()=[]
    ):

        ALLOWED_FILE_FORMATS = ['CSV','JSON','AVRO']
        if file_format not in ALLOWED_FILE_FORMATS:
            raise Exception(f'{file_format} file format not allowed. Allowed formats are: {", ".join(ALLOWED_FILE_FORMATS)}.')

        self.file_format = file_format
        self.container_name = container_name
        self.cached_table_name = cached_table_name
        self.datalake_path = datalake_path
        self.column_mappings = column_mappings


