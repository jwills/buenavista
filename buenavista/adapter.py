import json
import random
from typing import Dict, Iterator, List, Optional, Tuple


class QueryResult:
    """The BV representation of a result of a query."""

    def has_results(self) -> bool:
        raise NotImplementedError

    def column_count(self):
        raise NotImplementedError

    def column(self, index: int) -> Tuple[str, int]:
        raise NotImplementedError

    def rows(self) -> Iterator[List[Optional[str]]]:
        raise NotImplementedError


class AdapterHandle:
    def __init__(self, cursor):
        self.cursor = cursor
        self.process_id = random.randint(0, 2**31 - 1)
        self.secret_key = random.randint(0, 2**31 - 1)

    def close(self):
        self.cursor.close()

    def execute_sql(self, sql: str, params=None) -> QueryResult:
        raise NotImplementedError


class Adapter:
    """Translation layer from an upstream data source into the BV representation of a query result."""

    def create_handle(self) -> AdapterHandle:
        raise NotImplementedError

    def cancel_query(self, process_id: int, secret_key: int):
        print("Cancel request for process %d, secret key %d" % (process_id, secret_key))

    def parameters(self) -> Dict[str, str]:
        return {}
