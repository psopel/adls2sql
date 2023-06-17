import csv

class CSVDeserializer():

    def __init__(
            self,
            csv_data: str,
            headers_in_first_row: bool,
            file_format_options: dict={}
        ):

        print(file_format_options)
        separator = file_format_options.get("separator",";")
        quote = file_format_options.get("quote",'"')

        self.csv_data = csv_data
        self.headers_in_first_row = headers_in_first_row
        self.file_format_options = file_format_options

        print(f"Separator {separator}, quote {quote}")

        csv.register_dialect('default_csv', delimiter=separator, quotechar=quote)
        self.reader = csv.reader(csv_data.splitlines(), dialect='default_csv')

    def get_column_names(self):

        separator = self.file_format_options.get('separator',';')
        quote = self.file_format_options.get('quote','"')
            
        first_row = self.csv_data.split('\n')[0]

        if self.headers_in_first_row:
            return(first_row.split(separator))
        else:
            column_names = []
            i = 0
            for x in first_row.split(separator):
                i += 1
                column_names.append(f'col{str(i).zfill(3)}')
            return(column_names)

    def as_table(self):
        
        if self.headers_in_first_row:
            next(self.reader, None)
        res = [x for x in self.reader]
        return(res)
            

