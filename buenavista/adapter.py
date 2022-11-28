import datetime
import json
import random
from typing import Dict, List


class PGType:
    """Represents a PostgreSQL type and a function to convert a Python value to its wire format."""

    def __init__(self, oid, converter=None):
        self.oid = oid
        if not converter:
            self.converter = lambda x: str(x)
        else:
            self.converter = converter


BIGINT_TYPE = PGType(20)
BOOL_TYPE = PGType(16, lambda v: "true" if v else "false")
BYTES_TYPE = PGType(17, lambda v: "\\x" + v.hex())
DATE_TYPE = PGType(1082, lambda v: v.isoformat())
FLOAT_TYPE = PGType(701)
INTEGER_TYPE = PGType(23)
INTERVAL_TYPE = PGType(
    1186, lambda v: f"{v.days} days {v.seconds} seconds {v.microseconds} microseconds"
)
JSON_TYPE = PGType(114, lambda v: json.dumps(v))
NUMERIC_TYPE = PGType(1700)
NULL_TYPE = PGType(-1, lambda v: None)
TEXT_TYPE = PGType(25)
TIME_TYPE = PGType(1083, lambda v: v.isoformat())
TIMESTAMP_TYPE = PGType(1114, lambda v: v.isoformat())
UNKNOWN_TYPE = PGType(705)


class Field:
    """A name/type pair for a field in a PostgreSQL table."""

    def __init__(self, name, pg_type: PGType):
        self.name = name
        self.pg_type = pg_type

    @property
    def oid(self):
        return self.pg_type.oid

    def to_bytes(self, value) -> bytes:
        return self.pg_type.converter(value).encode("utf-8")


class QueryResult:
    """The BV representation of a result of a query."""

    def __init__(self, fields: List[Field], rows):
        self.fields = fields
        self.rows = rows


class AdapterHandle:
    def __init__(self, cursor, parent: "Adapter", process_id: int, secret_key: int):
        self.cursor = cursor
        self.parent = parent
        self.process_id = process_id
        self.secret_key = secret_key

    def close(self):
        self.cursor.close()

    def execute_sql(self, sql: str, params=None, limit: int = 0) -> QueryResult:
        print("Original SQL:", sql)
        sql = self.parent.rewrite_sql(sql)
        print("Rewritten SQL:", sql)
        if params:
            self.cursor.execute(sql, params)
        else:
            self.cursor.execute(sql)

        rows = []
        if self.cursor.description:
            if limit == 0:
                rows = self.cursor.fetchall()
            else:
                rows = self.cursor.fetchmany(limit)

        return self.parent.to_query_result(self.cursor.description, rows)


class Adapter:
    """Translation layer from an upstream data source into the BV representation of a query result."""

    def create_handle(self) -> AdapterHandle:
        return AdapterHandle(
            self._cursor(), self, random.randint(0, 1000000), random.randint(0, 1000000)
        )

    def _cursor(self):
        raise NotImplementedError

    def to_query_result(self, description, rows) -> QueryResult:
        raise NotImplementedError

    def cancel_query(self, process_id: int, secret_key: int):
        print("Cancel request for process %d, secret key %d" % (process_id, secret_key))

    def parameters(self) -> Dict[str, str]:
        return {}

    def rewrite_sql(self, sql: str) -> str:
        return sql
