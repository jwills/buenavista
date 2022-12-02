from typing import Dict

import psycopg2

from buenavista.adapter import *


class PGQueryResult(QueryResult):
    def __init__(
        self, fields: List[Tuple[str, PGType]], rows: List[List[Optional[str]]]
    ):
        self.fields = fields
        self.rows = rows

    def column_count(self):
        return len(self.fields)

    def column(self, index: int) -> Tuple[str, int]:
        return self.fields[index]

    def rows(self) -> Iterator[List[Optional[str]]]:
        for row in self.rows:
            ret = []
            for j, col in enumerate(self.fields):
                pv = row[j]
                if pv is None:
                    ret.append(pv)
                else:
                    ret.append(col.converter(pv))
            yield ret


class Psycopg2Adapter(Adapter):
    def __init__(self, **kwargs):
        self.db = psycopg2.connect(**kwargs)

    def _cursor(self):
        return self.db.cursor()

    def parameters(self) -> Dict[str, str]:
        return {
            "server_version": self.db.get_parameter_status("server_version"),
            "client_encoding": self.db.get_parameter_status("client_encoding"),
        }

    def execute_sql(self, cursor, sql: str, params=None) -> QueryResult:
        cursor.execute(sql, params)
        rows = []
        if cursor.description:
            rows = cursor.fetchall()
        return self.to_query_result(cursor.description, rows)

    def to_query_result(self, description, rows) -> QueryResult:
        if not description:
            return PGQueryResult([], [])

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
    server = BuenaVistaServer(address, Psycopg2Adapter(database="postgres"))
    ip, port = server.server_address
    print("Listening on {ip}:{port}".format(ip=ip, port=port))
    server.serve_forever()
