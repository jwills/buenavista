import logging
import os
from typing import Dict, Iterator, List, Optional, Tuple

import duckdb
import pyarrow as pa

from buenavista.adapter import Adapter, AdapterHandle, QueryResult
from buenavista.types import PGType, PGTypes


logger = logging.getLogger(__name__)


def pg_type(t: pa.DataType) -> PGType:
    if pa.types.is_boolean(t):
        return PGTypes.BOOL
    elif pa.types.is_int64(t):
        return PGTypes.BIGINT
    elif pa.types.is_integer(t):
        return PGTypes.INTEGER
    elif pa.types.is_string(t):
        return PGTypes.TEXT
    elif pa.types.is_date(t):
        return PGTypes.DATE
    elif pa.types.is_time(t):
        return PGTypes.TIME
    elif pa.types.is_timestamp(t):
        return PGTypes.TIMESTAMP
    elif pa.types.is_floating(t):
        return PGTypes.FLOAT
    elif pa.types.is_decimal(t):
        return PGTypes.NUMERIC
    elif pa.types.is_binary(t):
        return PGTypes.BYTES
    elif pa.types.is_interval(t):
        return PGTypes.INTERVAL
    elif pa.types.is_list(t) or pa.types.is_struct(t) or pa.types.is_map(t):
        # TODO: support detailed nested types
        return PGTypes.TEXT
    else:
        return PGTypes.UNKNOWN


class RecordBatchIterator(Iterator[List[Optional[str]]]):
    def __init__(self, rbr: pa.RecordBatchReader, pg_types: List[PGType]):
        self.rbr = rbr
        self.pg_types = pg_types

    def __iter__(self):
        try:
            self.rb = self.rbr.read_next_batch()
        except StopIteration:
            self.rb = None
        self.i = 0
        return self

    def __next__(self) -> List[Optional[str]]:
        if self.rb is None:
            raise StopIteration
        if self.i >= self.rb.num_rows:
            self.rb = self.rbr.read_next_batch()
            self.i = 0
        ret = []
        for j, col in enumerate(self.rb.columns):
            pv = col[self.i].as_py()
            if pv is None:
                ret.append(pv)
            else:
                ret.append(self.pg_types[j].converter(pv))
        self.i += 1
        return ret


class DuckDBQueryResult(QueryResult):
    def __init__(self, rbr: Optional[pa.RecordBatchReader] = None):
        if rbr:
            self.rbr = rbr
            self.pg_types = [pg_type(s.type) for s in rbr.schema]
        else:
            self.rbr = None
            self.pg_types = []

    def has_results(self) -> bool:
        return self.rbr is not None

    def column_count(self):
        if self.rbr:
            return len(self.rbr.schema)
        else:
            return 0

    def column(self, index: int) -> Tuple[str, int]:
        if self.rbr:
            s = self.rbr.schema[index]
            return s.name, self.pg_types[index].oid
        else:
            raise IndexError("No column at index %d" % index)

    def rows(self) -> Iterator[List[Optional[str]]]:
        if self.rbr:
            return RecordBatchIterator(self.rbr, self.pg_types)
        else:
            return iter([])


class DuckDBAdapterHandle(AdapterHandle):
    def __init__(self, cursor):
        super().__init__()
        self._cursor = cursor
        self.in_txn = False
        self.refresh_config()

    def cursor(self):
        return self._cursor

    def close(self):
        self._cursor.close()

    def refresh_config(self):
        self.config_params = set(
            [
                r[0]
                for r in self._cursor.execute(
                    "SELECT name FROM duckdb_settings()"
                ).fetchall()
            ]
        )

    def load_df_function(self, table: str):
        return self._cursor.query(f"select * from {table}")

    def rewrite_sql(self, sql: str) -> str:
        """Some minimalist SQL rewrites, inspired by postlite, to make DBeaver less unhappy."""
        if sql.startswith("SET "):
            tokens = sql.split()
            if tokens[1].lower() in self.config_params:
                return sql
            else:
                return ""
        if sql == "SHOW search_path":
            return "SELECT 'public' as search_path"
        elif sql == "SHOW TRANSACTION ISOLATION LEVEL":
            return "SELECT 'read committed' as transaction_isolation"
        elif "::regclass" in sql:
            return sql.replace("::regclass", "")
        elif "::regtype" in sql:
            return sql.replace("::regtype", "")
        elif "pg_get_expr(ad.adbin, ad.adrelid, true)" in sql:
            return sql.replace(
                "pg_get_expr(ad.adbin, ad.adrelid, true)",
                "pg_get_expr(ad.adbin, ad.adrelid)",
            )
        return sql

    def in_transaction(self) -> bool:
        return self.in_txn

    def execute_sql(self, sql: str, params=None) -> QueryResult:
        lsql = sql.lower()
        if self.in_txn:
            if "commit" in lsql:
                self.in_txn = False
            elif "rollback" in lsql:
                self.in_txn = False
            elif "begin" in lsql:
                return DuckDBQueryResult()
        elif "begin" in lsql:
            self.in_txn = True

        logger.debug("Original SQL: %s", sql)
        sql = self.rewrite_sql(sql)
        logger.debug("Rewritten SQL: %s", sql)
        if params:
            self._cursor.execute(sql, params)
        else:
            self._cursor.execute(sql)

        rb = None
        if self._cursor.description:
            if (
                "select" in lsql
                or "with" in lsql
                or "describe" in lsql
                or "show" in lsql
            ):
                rb = self._cursor.fetch_record_batch()
            elif "load " in lsql:
                self.refresh_config()
        return DuckDBQueryResult(rb)


class DuckDBAdapter(Adapter):
    def __init__(self, db):
        super().__init__()
        self.db = db
        self._do_setup()

    def _do_setup(self):
        # some tables/view definitions we need for DBeaver
        if duckdb.__version__ < "0.6.1":
            self.db.execute(
                "CREATE OR REPLACE VIEW pg_catalog.pg_database AS SELECT 0 oid, 'main' datname"
            )
            self.db.execute(
                "CREATE OR REPLACE VIEW pg_catalog.pg_proc AS SELECT cast(floor(1000000*random()) as bigint) oid, function_name proname, s.oid pronamespace, return_type prorettype, parameters proargnames, function_type = 'aggregate' proisagg, function_type = 'table' proretset FROM duckdb_functions() f LEFT JOIN duckdb_schemas() s USING (schema_name)"
            )
            self.db.execute(
                "CREATE OR REPLACE VIEW pg_catalog.pg_settings AS SELECT name, value setting, description short_desc, CASE WHEN input_type = 'VARCHAR' THEN 'string' WHEN input_type = 'BOOLEAN' THEN 'bool' WHEN input_type IN ('BIGINT', 'UBIGINT') THEN 'integer' ELSE input_type END vartype FROM duckdb_settings()"
            )

        self.db.execute(
            "CREATE OR REPLACE FUNCTION pg_catalog.pg_total_relation_size(oid) AS (SELECT estimated_size FROM duckdb_tables() WHERE table_oid = oid)"
        )
        self.db.execute(
            "CREATE OR REPLACE FUNCTION pg_catalog.pg_relation_size(oid) AS (SELECT estimated_size FROM duckdb_tables() WHERE table_oid = oid)"
        )

    def parameters(self) -> Dict[str, str]:
        return {
            "server_version": "postduck.0.6",
            "client_encoding": "UTF8",
            "DateStyle": "ISO",
        }

    def new_handle(self) -> AdapterHandle:
        return DuckDBAdapterHandle(self.db.cursor())


if __name__ == "__main__":
    from buenavista.core import BuenaVistaServer
    import sys

    if len(sys.argv) < 2:
        print("Using in-memory DuckDB database")
        db = duckdb.connect()
    else:
        print("Using DuckDB database at", sys.argv[1])
        db = duckdb.connect(sys.argv[1])

    bv_host = "0.0.0.0"
    bv_port = 5433

    if "BUENAVISTA_HOST" in os.environ:
        bv_host = os.environ["BUENAVISTA_HOST"]

    if "BUENAVISTA_PORT" in os.environ:
        bv_port = int(os.environ["BUENAVISTA_PORT"])

    address = (bv_host, bv_port)

    server = BuenaVistaServer(address, DuckDBAdapter(db))
    ip, port = server.server_address
    print("Listening on {ip}:{port}".format(ip=ip, port=port))

    try:
        server.serve_forever()
    finally:
        server.shutdown()
        db.close()
