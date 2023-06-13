import csv

class CSVDeserializer():

    def __init__(self, csv_data: str, headers_in_first_row: bool):

        self.csv_data = csv_data
        self.headers_in_first_row = headers_in_first_row
        csv.register_dialect('default_csv', delimiter=';', quotechar='"')
        self.reader = csv.reader(csv_data.splitlines(), dialect='default_csv')

    def get_column_names(self):
            
        first_row = self.csv_data.split('\n')[0]

        if self.headers_in_first_row:
            return(first_row.split(';'))
        else:
            column_names = []
            i = 0
            for x in first_row.split(';'):
                i += 1
                column_names.append(f'col{str(i).zfill(3)}')
            return(column_names)

    def as_table(self):

        """
        res = []
        
        rows = self.csv_data.split('\n')
        if self.headers_in_first_row:
            rows = rows[1:]
        for row in rows:
            res.append(row.split(';'))
        """
        
        if self.headers_in_first_row:
            next(self.reader, None)
        res = [x for x in self.reader]
        return(res)
            

