from typing import Dict, List, Optional, Tuple

import duckdb
import pyarrow as pa

from buenavista.core import BVBuffer
from buenavista.adapter import *


def pg_type(t: pa.DataType) -> PGType:
    if pa.types.is_boolean(t):
        return BOOL_TYPE
    elif pa.types.is_int64(t):
        return BIGINT_TYPE
    elif pa.types.is_integer(t):
        return INTEGER_TYPE
    elif pa.types.is_string(t):
        return TEXT_TYPE
    elif pa.types.is_date(t):
        return DATE_TYPE
    elif pa.types.is_time(t):
        return TIME_TYPE
    elif pa.types.is_timestamp(t):
        return TIMESTAMP_TYPE
    elif pa.types.is_floating(t):
        return FLOAT_TYPE
    elif pa.types.is_decimal(t):
        return NUMERIC_TYPE
    elif pa.types.is_binary(t):
        return BYTES_TYPE
    elif pa.types.is_interval(t):
        return INTERVAL_TYPE
    elif pa.types.is_list(t) or pa.types.is_struct(t) or pa.types.is_map(t):
        # TODO: support detailed nested types
        return TEXT_TYPE
    else:
        return UNKNOWN_TYPE


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


class DuckDBAdapter(Adapter):
    def __init__(self, db):
        self.db = db

    def _cursor(self):
        return self.db.cursor()

    def parameters(self) -> Dict[str, str]:
        return {
            "server_version": "postduck.0.6",
            "client_encoding": "UTF8",
        }

    def rewrite_sql(self, sql: str) -> str:
        """Some minimalist SQL rewrites, inspired by postlite, to make DBeaver less unhappy."""
        if sql.startswith("SET "):
            return ""
        elif sql == "SHOW search_path":
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

    def execute_sql(self, cursor, sql: str, params=None) -> QueryResult:
        print("Original SQL:", sql)
        sql = self.rewrite_sql(sql)
        print("Rewritten SQL:", sql)
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        rb = None
        if cursor.description:
            rb = cursor.fetch_record_batch()
        return DuckDBQueryResult(rb)


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
        "CREATE OR REPLACE VIEW pg_catalog.pg_proc AS SELECT cast(floor(1000000*random()) as bigint) oid, function_name proname, s.oid pronamespace, return_type prorettype, parameters proargnames, function_type = 'aggregate' proisagg, function_type = 'table' proretset FROM duckdb_functions() f LEFT JOIN duckdb_schemas() s USING (schema_name)"
    )
    db.execute(
        "CREATE OR REPLACE VIEW pg_catalog.pg_settings AS SELECT name, value setting, description short_desc, CASE WHEN input_type = 'VARCHAR' THEN 'string' WHEN input_type = 'BOOLEAN' THEN 'bool' WHEN input_type IN ('BIGINT', 'UBIGINT') THEN 'integer' ELSE input_type END vartype FROM duckdb_settings()"
    )
    db.execute(
        "CREATE OR REPLACE FUNCTION pg_catalog.pg_total_relation_size(oid) AS (SELECT estimated_size FROM duckdb_tables() WHERE table_oid = oid)"
    )
    db.execute(
        "CREATE OR REPLACE FUNCTION pg_catalog.pg_relation_size(oid) AS (SELECT estimated_size FROM duckdb_tables() WHERE table_oid = oid)"
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
