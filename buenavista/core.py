import random
from typing import Any, Dict, Iterator, List, Optional, Tuple

from .types import PGType


class QueryResult:
    """The BV representation of a result of a query."""

    def has_results(self) -> bool:
        raise NotImplementedError

    def column_count(self):
        raise NotImplementedError

    def column(self, index: int) -> Tuple[str, PGType]:
        raise NotImplementedError

    def rows(self) -> Iterator[List]:
        raise NotImplementedError

    def status(self) -> str:
        raise NotImplementedError


class Session:
    def __init__(self):
        self.process_id = random.randint(0, 2**31 - 1)
        self.secret_key = random.randint(0, 2**31 - 1)

    def cursor(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def execute_sql(self, sql: str, params=None) -> QueryResult:
        raise NotImplementedError

    def in_transaction(self) -> bool:
        raise NotImplementedError

    def load_df_function(self, table: str):
        raise NotImplementedError


class Connection:
    """Translation layer from an upstream data source into the BV representation of a query result."""

    def __init__(self):
        self._sessions = {}

    def create_session(self) -> Session:
        sess = self.new_session()
        self._sessions[sess.process_id] = sess
        return sess

    def get_session(self, process_id: int) -> Optional[Session]:
        return self._sessions.get(process_id)

    def close_session(self, session: Session):
        if session and session.process_id in self._sessions:
            del self._sessions[session.process_id]
            session.close()

    def new_session(self) -> Session:
        raise NotImplementedError

    def parameters(self) -> Dict[str, str]:
        return {}


class Extension:
    def type(self) -> str:
        raise NotImplementedError

    def apply(self, params: dict, session: Session) -> QueryResult:
        raise NotImplementedError


class SimpleQueryResult(QueryResult):
    def __init__(self, name: str, value: Any, type: PGType):
        self.name = name
        self.value = str(value)
        self.type = type

    def has_results(self):
        return True

    def column_count(self):
        return 1

    def column(self, index: int) -> Tuple[str, int]:
        if index == 0:
            return (self.name, self.type.oid)
        else:
            raise IndexError

    def rows(self) -> Iterator[List[Optional[str]]]:
        return iter([self.value])

    def status(self) -> str:
        return ""
