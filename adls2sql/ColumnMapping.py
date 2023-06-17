class ColumnMapping():

    def __init__(
        self,
        ordinal_position: int,
        column_name: str,
        column_type: str      
    ):
        
        self.ordinal_position = ordinal_position
        self.column_name = column_name
        self.column_type = column_type