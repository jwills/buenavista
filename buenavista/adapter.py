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

    def load_df_function(self, table: str):
        raise NotImplementedError


class Adapter:
    """Translation layer from an upstream data source into the BV representation of a query result."""

    def __init__(self):
        self._handles = {}

    def create_handle(self) -> AdapterHandle:
        handle = self.new_handle()
        self._handles[handle.process_id] = handle
        return handle

    def get_handle(self, process_id: int) -> Optional[AdapterHandle]:
        return self._handles.get(process_id)

    def close_handle(self, handle: AdapterHandle):
        if handle and handle.process_id in self._handles:
            del self._handles[handle.process_id]
            handle.close()

    def new_handle(self) -> AdapterHandle:
        raise NotImplementedError

    def parameters(self) -> Dict[str, str]:
        return {}
