from os import listdir
from os.path import isfile, join


class SqlFileReader:
    def __init__(self, pg) -> None:
        self.pg = pg

    def sql_init(self, path: str) -> None:
        files = [f for f in listdir(path) if isfile(join(path, f))]

        for fl in files:
            with open(f"{path}/{fl}", 'r') as f:
                sql = f.read()

            with self.pg.connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(sql)
