import logging
import re
from typing import Dict, Iterator, List, Optional, Tuple

import pyarrow as pa
import sqlglot

from buenavista.core import BVType, Connection, QueryResult, Session


logger = logging.getLogger(__name__)


def to_bvtype(t: pa.DataType) -> BVType:
    if pa.types.is_int64(t):
        return BVType.BIGINT
    elif pa.types.is_integer(t):
        return BVType.INTEGER
    elif pa.types.is_string(t) or pa.types.is_large_string(t):
        return BVType.TEXT
    elif pa.types.is_date(t):
        return BVType.DATE
    elif pa.types.is_time(t):
        return BVType.TIME
    elif pa.types.is_timestamp(t):
        return BVType.TIMESTAMP
    elif pa.types.is_floating(t):
        return BVType.FLOAT
    elif pa.types.is_decimal(t):
        return BVType.DECIMAL
    elif pa.types.is_binary(t):
        return BVType.BYTES
    elif pa.types.is_boolean(t):
        return BVType.BOOL
    elif pa.types.is_interval(t):
        return BVType.INTERVAL
    elif pa.types.is_list(t):
        field_type = t.field(0).type
        if pa.types.is_integer(field_type):
            return BVType.INTEGERARRAY
        elif pa.types.is_string(field_type):
            return BVType.STRINGARRAY
        else:
            # TODO: detailed nested types
            return BVType.JSON
    elif pa.types.is_struct(t) or pa.types.is_map(t):
        # TODO: support detailed nested types
        return BVType.JSON
    else:
        raise Exception("Could not convert DuckDB type: " + str(t))


class RecordBatchIterator(Iterator[List[Optional[str]]]):
    def __init__(self, rbr: pa.RecordBatchReader):
        self.rbr = rbr

    def __iter__(self):
        try:
            self.rb = self.rbr.read_next_batch()
        except StopIteration:
            self.rb = None
        self.i = 0
        return self

    def __next__(self) -> List:
        if self.rb is None:
            raise StopIteration
        if self.i >= self.rb.num_rows:
            self.rb = self.rbr.read_next_batch()
            self.i = 0
        ret = []
        for col in self.rb.columns:
            ret.append(col[self.i].as_py())
        self.i += 1
        return ret


class DuckDBQueryResult(QueryResult):
    def __init__(
        self, rbr: Optional[pa.RecordBatchReader] = None, status: Optional[str] = None
    ):
        super().__init__()
        if rbr:
            self.rbr = rbr
            self.bvtypes = [to_bvtype(s.type) for s in rbr.schema]
        else:
            self.rbr = None
            self.bvtypes = []
        self._status = status

    def has_results(self) -> bool:
        return self.rbr is not None

    def column_count(self):
        if self.rbr:
            return len(self.rbr.schema)
        else:
            return 0

    def column(self, index: int) -> Tuple[str, BVType]:
        if self.rbr:
            s = self.rbr.schema[index]
            return s.name, self.bvtypes[index]
        else:
            raise IndexError("No column at index %d" % index)

    def rows(self) -> Iterator[List]:
        if self.rbr:
            return RecordBatchIterator(self.rbr)
        else:
            return iter([])

    def status(self) -> str:
        return self._status


class DuckDBSession(Session):
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
        if match := re.search(r"PREPARE\s+(\w+)\s+FROM", sql):
            stmt = match.group(1)
            target = f"PREPARE {stmt} FROM"
            replace = f"PREPARE {stmt} AS"
            return sql.replace(target, replace)
        if sql.startswith("SET "):
            tokens = sql.split()
            if tokens[1].lower() in self.config_params:
                return sql
            else:
                return ""
        elif sql == "SHOW search_path":
            return "SELECT current_setting('search_path') as search_path"
        elif sql == "SHOW TRANSACTION ISOLATION LEVEL":
            return "SELECT 'read committed' as transaction_isolation"
        elif sql == "BEGIN READ ONLY":
            return "BEGIN"
        elif (
            sql
            == "SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys'"
        ):
            return "SELECT 32 as setting"
        elif "::regclass" in sql:
            return sql.replace("::regclass", "")
        elif "::regtype" in sql:
            return sql.replace("::regtype", "")
        elif "::regproc" in sql:
            return sql.replace("::regproc", "")
        elif "pg_get_expr(ad.adbin, ad.adrelid, true)" in sql:
            return sql.replace(
                "pg_get_expr(ad.adbin, ad.adrelid, true)",
                "pg_get_expr(ad.adbin, ad.adrelid)",
            )
        elif "pg_catalog.current_schemas" in sql:
            return sql.replace("pg_catalog.current_schemas", "current_schemas")
        elif "pg_catalog.generate_series" in sql:
            return sql.replace("pg_catalog.generate_series", "generate_series")
        return sql

    def in_transaction(self) -> bool:
        return self.in_txn

    def execute_sql(self, sql: str, params=None) -> QueryResult:
        status = ""
        try:
            lsql = sqlglot.parse_one(sql).sql(comments=False)
        except:
            # TODO: log this
            lsql = sql

        lsql = lsql.lower()
        if self.in_txn:
            if "commit" in lsql:
                self.in_txn = False
                status = "COMMIT"
            elif "rollback" in lsql:
                self.in_txn = False
                status = "ROLLBACK"
            elif "begin" in lsql or "start transaction" in lsql:
                return DuckDBQueryResult(status="BEGIN")
        elif "begin" in lsql or "start transaction" in lsql:
            lsql = "begin"
            self.in_txn = True
            status = "BEGIN"

        logger.debug("Original SQL: %s", sql)
        sql = self.rewrite_sql(sql)
        logger.debug("Rewritten SQL: %s", sql)
        if params:
            self._cursor.execute(sql, params)
        else:
            self._cursor.execute(sql)

        if status:
            return DuckDBQueryResult(status=status)

        rb = None
        if self._cursor.description:
            if "load " in lsql:
                self.refresh_config()
                status = "LOAD"
            elif not ("insert " in lsql or "update " in lsql or "delete " in lsql):
                rb = self._cursor.fetch_record_batch()
        return DuckDBQueryResult(rb, status)


class DuckDBConnection(Connection):
    def __init__(self, db):
        super().__init__()
        self.db = db

    def parameters(self) -> Dict[str, str]:
        return {
            "server_version": "9.3.duckdb",
            "client_encoding": "UTF8",
            "DateStyle": "ISO",
        }

    def new_session(self) -> Session:
        cursor = self.db.cursor()
        cursor.execute("SET search_path='main'")
        return DuckDBSession(cursor)
