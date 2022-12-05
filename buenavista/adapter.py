import datetime
import json
import random
from typing import Dict, Iterator, List, Optional, Tuple


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
TIMESTAMP_TYPE = PGType(1114, lambda v: v.isoformat().replace("T", " "))
UNKNOWN_TYPE = PGType(705)


class QueryResult:
    """The BV representation of a result of a query."""

    def column_count(self):
        raise NotImplementedError

    def column(self, index: int) -> Tuple[str, int]:
        raise NotImplementedError

    def rows(self) -> Iterator[List[Optional[str]]]:
        raise NotImplementedError


class AdapterHandle:
    def __init__(self, cursor, parent: "Adapter", process_id: int, secret_key: int):
        self.cursor = cursor
        self.parent = parent
        self.process_id = process_id
        self.secret_key = secret_key

    def close(self):
        self.cursor.close()

    def execute_sql(self, sql: str, params=None) -> QueryResult:
        return self.parent.execute_sql(self.cursor, sql, params)


class Adapter:
    """Translation layer from an upstream data source into the BV representation of a query result."""

    def create_handle(self) -> AdapterHandle:
        return AdapterHandle(
            self._cursor(), self, random.randint(0, 1000000), random.randint(0, 1000000)
        )

    def _cursor(self):
        raise NotImplementedError

    def execute_sql(self, cursor, sql: str, params=None) -> QueryResult:
        raise NotImplementedError

    def cancel_query(self, process_id: int, secret_key: int):
        print("Cancel request for process %d, secret key %d" % (process_id, secret_key))

    def parameters(self) -> Dict[str, str]:
        return {}
