import json

class JSONDeserializer():

    def __init__(self, json_data: str):

        self.json = json.loads(json_data)


    def get_column_names(self) -> list:
        return(list(self.json[0].keys()))


    def as_table(self) -> list():
        res = []
        column_names = self.get_column_names()

        for x in self.json:
            row = []
            for c in column_names:
                row.append(x[c])
            res.append(row)

        return(res)

