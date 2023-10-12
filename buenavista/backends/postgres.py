import io
import re
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pandas as pd
import psycopg
from psycopg_pool import ConnectionPool

from buenavista.core import BVType, Connection, QueryResult, Session


OID_TO_BVTYPE = {
    -1: BVType.NULL,
    16: BVType.BOOL,
    17: BVType.BYTES,
    20: BVType.BIGINT,
    23: BVType.INTEGER,
    25: BVType.TEXT,
    114: BVType.JSON,
    701: BVType.FLOAT,
    705: BVType.UNKNOWN,
    1082: BVType.DATE,
    1083: BVType.TIME,
    1114: BVType.TIMESTAMP,
    1186: BVType.INTERVAL,
    1700: BVType.DECIMAL,
}


class PGQueryResult(QueryResult):
    def __init__(
        self,
        fields: List[Tuple[str, BVType]],
        rows: List[List[Optional[Any]]],
        status: Optional[str] = None,
    ):
        super().__init__()
        self.fields = fields
        self._rows = rows
        self._status = status

    def has_results(self) -> bool:
        return bool(self.fields)

    def column_count(self):
        return len(self.fields)

    def column(self, index: int) -> Tuple[str, BVType]:
        return self.fields[index]

    def rows(self) -> Iterator[List]:
        return iter(self._rows)

    def status(self):
        return self._status


class PGSession(Session):
    def __init__(self, parent, conn):
        super().__init__()
        self.parent = parent
        self.conn = conn
        self._cursor = conn.cursor()

    def close(self):
        self._cursor.close()
        self.parent.release(self.conn)
        self.conn = None

    def cursor(self):
        return self._cursor

    def execute_sql(self, sql: str, params=None) -> QueryResult:
        if params:
            sql = re.sub(r"\$\d+", r"%s", sql)
            self._cursor.execute(sql, params)
        else:
            self._cursor.execute(sql)
        status = self._cursor.statusmessage
        if self._cursor.description:
            rows = self._cursor.fetchall()
            res = self.to_query_result(self._cursor.description, rows, status)
        else:
            res = PGQueryResult([], [], status=status)
        return res

    def in_transaction(self) -> bool:
        return self.conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE

    def load_df_function(self, table: str):
        copy_query = f"COPY {table} TO STDOUT WITH CSV DELIMITER ',' HEADER"
        out = io.StringIO()
        with self._cursor.copy(copy_query) as copy:
            while data := copy.read():
                out.write(str(data, "utf8"))
        out.seek(0)
        return pd.read_csv(out)

    def to_query_result(self, description, rows, status) -> QueryResult:
        fields = []
        for d in description:
            name, oid = d[0], d[1]
            f = (name, OID_TO_BVTYPE.get(oid, BVType.UNKNOWN))
            fields.append(f)
        return PGQueryResult(fields, rows, status)


class PGConnection(Connection):
    def __init__(self, conninfo="", **kwargs):
        super().__init__()
        self.pool = ConnectionPool(psycopg.conninfo.make_conninfo(conninfo, **kwargs))

    def new_session(self) -> Session:
        conn = self.pool.getconn()
        conn.autocommit = True
        return PGSession(self, conn)

    def release(self, conn):
        self.pool.putconn(conn)

    def parameters(self) -> Dict[str, str]:
        return {"server_version": "9.3.bvproxy", "client_encoding": "UTF8"}
