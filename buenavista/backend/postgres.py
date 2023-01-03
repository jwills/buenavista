import os
import re
from typing import Dict, Iterator, List, Optional, Tuple

import psycopg
from psycopg_pool import ConnectionPool

from buenavista.adapter import Adapter, AdapterHandle, QueryResult
from buenavista.types import PGType


class PGQueryResult(QueryResult):
    def __init__(
        self, fields: List[Tuple[str, PGType]], rows: List[List[Optional[str]]]
    ):
        self.fields = fields
        self._rows = rows

    def has_results(self) -> bool:
        return bool(self.fields)

    def column_count(self):
        return len(self.fields)

    def column(self, index: int) -> Tuple[str, int]:
        field = self.fields[index]
        return (field[0], field[1].oid)

    def rows(self) -> Iterator[List[Optional[str]]]:
        def t(row):
            return [
                v if v is None else self.fields[i][1].converter(v)
                for i, v in enumerate(row)
            ]

        return iter(map(t, self._rows))


class PGAdapterHandle(AdapterHandle):
    def __init__(self, adapter, conn):
        super().__init__()
        self.adapter = adapter
        self.conn = conn

    def close(self):
        self.adapter.release(self.conn)
        self.conn = None

    def execute_sql(self, sql: str, params=None) -> QueryResult:
        cursor = self.conn.cursor()
        if params:
            sql = re.sub(r"\$\d+", r"%s", sql)
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        if cursor.description:
            rows = cursor.fetchall()
            res = self.to_query_result(cursor.description, rows)
        else:
            res = PGQueryResult([], [])
        cursor.close()
        return res

    def in_transaction(self) -> bool:
        return self.conn.status == psycopg.extensions.STATUS_IN_TRANSACTION

    def to_query_result(self, description, rows) -> QueryResult:
        fields = []
        for d in description:
            name, oid = d[0], d[1]
            f = (name, PGType.find_by_oid(oid))
            fields.append(f)
        return PGQueryResult(fields, rows)


class PGAdapter(Adapter):
    def __init__(self, conninfo="", **kwargs):
        super().__init__()
        self.pool = ConnectionPool(psycopg.conninfo.make_conninfo(conninfo, **kwargs))

    def new_handle(self) -> AdapterHandle:
        return PGAdapterHandle(self, self.pool.getconn())

    def release(self, conn):
        self.pool.putconn(conn)

    def parameters(self) -> Dict[str, str]:
        return {"server_version": "BV.psycopg2.1", "client_encoding": "UTF8"}


if __name__ == "__main__":
    from buenavista.core import BuenaVistaServer
    from buenavista.extensions.dbt import DbtPythonRunner

    address = ("localhost", 5433)
    server = BuenaVistaServer(
        address,
        PGAdapter(
            conninfo="",
            host="localhost",
            port=5432,
            user=os.getenv("USER"),
            dbname="postgres",
        ),
        extensions=[DbtPythonRunner()],
    )
    ip, port = server.server_address
    print("Listening on {ip}:{port}".format(ip=ip, port=port))
    server.serve_forever()
