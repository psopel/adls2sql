from avro.datafile import DataFileReader
from avro.io import DatumReader

class AvroDeserializer():

    def __init__(self, avro_data: bytes):

        self.avro_data = avro_data

    def get_column_names(self):

        reader = DataFileReader(self.avro_data, DatumReader())
        for row in reader:
            return(list(row.keys()))

    def as_table(self) -> list():
        res = []
        column_names = self.get_column_names()

        reader = DataFileReader(self.avro_data, DatumReader())
        for x in reader:
            row = []
            for c in column_names:
                row.append(x[c])
            res.append(row)
        
        return(res)
        


