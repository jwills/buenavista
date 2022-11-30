from typing import Dict

import duckdb
import pyarrow as pa

from buenavista.core import BVBuffer
from buenavista.adapter import *


def oid(t: pa.DataType) -> int:
    if pa.lib.is_boolean(t):
        return BOOL_TYPE.oid
    elif pa.lib.is_integer(t):
        return INTEGER_TYPE.oid
    elif pa.lib.is_string(t):
        return TEXT_TYPE.oid
    elif pa.lib.is_date(t):
        return DATE_TYPE.oid
    elif pa.lib.is_time(t):
        return TIME_TYPE.oid
    elif pa.lib.is_timestamp(t):
        return TIMESTAMP_TYPE.oid
    elif pa.lib.is_float(t):
        return FLOAT_TYPE.oid
    elif pa.lib.is_decimal(t):
        return NUMERIC_TYPE.oid
    elif pa.lib.is_binary(t):
        return BYTES_TYPE.oid
    else:
        return UNKNOWN_TYPE.oid


class DuckDBQueryResult(QueryResult):
    def __init__(self, tbl: pa.Table):
        self.tbl = tbl

    def row_count(self):
        return self.tbl.num_rows

    def column_count(self):
        return self.tbl.num_columns

    def column(self, index: int) -> Tuple[str, int]:
        s = self.tbl.schema[index]
        return s.name, oid(s.type)

    def row(self, index: int) -> bytes:
        row = self.tbl.slice(offset=index, length=1)
        buf = BVBuffer()
        # TODO: fill this block in here
        return buf.get_value()


class DuckDBAdapter(Adapter):
    def __init__(self, db):
        self.db = db

    def _cursor(self):
        return self.db.cursor()

    def parameters(self) -> Dict[str, str]:
        return {
            "server_version": "quackity.quack.quack",
            "client_encoding": "UTF8",
        }

    def rewrite_sql(self, sql: str) -> str:
        """Some minimalist SQL rewrites, inspired by postlite, to make DBeaver less unhappy."""
        if sql.startswith("SET "):
            return "SELECT 'SET'"
        elif "::regclass" in sql:
            # such a hack but it turns out to work fine here
            return sql.replace("::regclass", "")
        elif sql == "SHOW search_path":
            return "SELECT 'public' as search_path"
        elif sql == "SHOW TRANSACTION ISOLATION LEVEL":
            return "SELECT 'read committed' as transaction_isolation"
        return sql

    def execute_sql(
        self, cursor, sql: str, params=None, limit: int = -1
    ) -> QueryResult:
        print("Original SQL:", sql)
        sql = self.rewrite_sql(sql)
        print("Rewritten SQL:", sql)
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        tbl = cursor.fetch_arrow_table()
        return DuckDBQueryResult(tbl)


if __name__ == "__main__":
    from buenavista.core import BuenaVistaServer
    import sys

    if len(sys.argv) < 2:
        print("Using in-memory DuckDB database")
        db = duckdb.connect()
    else:
        print("Using DuckDB database at", sys.argv[1])
        db = duckdb.connect(sys.argv[1])

    # some tables/view definitions we need for DBeaver
    db.execute(
        "CREATE OR REPLACE VIEW pg_catalog.pg_database AS SELECT 0 oid, 'main' datname"
    )
    db.execute(
        "CREATE OR REPLACE VIEW pg_catalog.pg_proc AS SELECT cast(floor(1000000*random()) as bigint) oid, function_name proname, s.oid pronamespace FROM duckdb_functions() f LEFT JOIN duckdb_schemas() s USING (schema_name)"
    )
    db.execute(
        "CREATE OR REPLACE VIEW pg_catalog.pg_settings AS SELECT name, value setting, description short_desc, CASE WHEN input_type = 'VARCHAR' THEN 'string' WHEN input_type = 'BOOLEAN' THEN 'bool' WHEN input_type IN ('BIGINT', 'UBIGINT') THEN 'integer' ELSE input_type END vartype FROM duckdb_settings()"
    )

    address = ("localhost", 5433)
    server = BuenaVistaServer(address, DuckDBAdapter(db))
    ip, port = server.server_address
    print("Listening on {ip}:{port}".format(ip=ip, port=port))
    try:
        server.serve_forever()
    finally:
        server.shutdown()
        db.close()
