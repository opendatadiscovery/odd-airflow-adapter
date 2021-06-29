from .. import settings

from oddrn import Generator

generator = Generator(data_source=settings.POSTGRES_SOURCE, cloud=settings.CLOUD)

def get_table_oddrn(db:str, schema: str, table: str) -> str:
    return generator.get_table(db, schema,  table)
