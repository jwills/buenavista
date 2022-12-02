from typing import Dict

import pg8000.dbapi
from buenavista.adapter import *


class PGQueryResult(QueryResult):
    def __init__(
        self, fields: List[Tuple[str, PGType]], rows: List[List[Optional[str]]]
    ):
        self.fields = fields
        self._rows = rows

    def column_count(self):
        return len(self.fields)

    def column(self, index: int) -> Tuple[str, int]:
        field = self.fields[index]
        return (field[0], field[1].oid)

    def rows(self) -> Iterator[List[Optional[str]]]:
        def t(row):
            return [self.fields[i][1].converter(v) for i, v in enumerate(row)]

        return iter(map(t, self._rows))


class PGAdapter(Adapter):
    pg8000.dbapi.paramstyle = "numeric"

    def __init__(self, **kwargs):
        self.db = pg8000.dbapi.connect(**kwargs)
        self.db.autocommit = True

    def _cursor(self):
        return self.db.cursor()

    def parameters(self) -> Dict[str, str]:
        return {"server_version": "BV.pg8000.1", "client_encoding": "UTF8"}

    def execute_sql(self, cursor, sql: str, params=None) -> QueryResult:
        if "$" in sql:
            sql = sql.replace("$", ":")
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        if cursor.description:
            rows = cursor.fetchall()
            return self.to_query_result(cursor.description, rows)
        else:
            return PGQueryResult([], [])

    def to_query_result(self, description, rows) -> QueryResult:
        fields = []
        for d in description:
            name, oid = d[0], d[1]
            f = None
            if oid == BOOL_TYPE.oid:
                f = (name, BOOL_TYPE)
            elif oid == TEXT_TYPE.oid:
                f = (name, TEXT_TYPE)
            elif oid == NUMERIC_TYPE.oid:
                f = (name, NUMERIC_TYPE)
            elif oid == TIMESTAMP_TYPE.oid:
                f = (name, TIMESTAMP_TYPE)
            elif oid == INTERVAL_TYPE.oid:
                f = (name, INTERVAL_TYPE)
            elif oid == TIME_TYPE.oid:
                f = (name, TIME_TYPE)
            elif oid == DATE_TYPE.oid:
                f = (name, DATE_TYPE)
            elif oid == BYTES_TYPE.oid:
                f = (name, BYTES_TYPE)
            elif oid == INTEGER_TYPE.oid:
                f = (name, INTEGER_TYPE)
            else:
                f = (name, UNKNOWN_TYPE)
            fields.append(f)
        return PGQueryResult(fields, rows)


if __name__ == "__main__":
    from buenavista.core import BuenaVistaServer

    address = ("localhost", 5433)
    server = BuenaVistaServer(
        address,
        PGAdapter(host="localhost", port=5432, user="postgres", database="postgres"),
    )
    ip, port = server.server_address
    print("Listening on {ip}:{port}".format(ip=ip, port=port))
    server.serve_forever()
