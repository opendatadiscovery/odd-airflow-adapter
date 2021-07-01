from ..oddrn.postgres import get_table_oddrn

class PostgresExtractor:
    def __init__(self, task):
        self._db = task.postgres_conn_id

    def get_oddrn_list(self, tables):
        response = []
        for table in tables:
            source = table.split(".")
            if len(source) > 1:
                # Schema included
                oddrn = get_table_oddrn(self._db, source[0],  source[1])
            else:
                oddrn = get_table_oddrn(self._db, "public", source[0])
            response.append(oddrn)
        return response