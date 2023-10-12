import enum
import json
import re
import uuid
from typing import Any, Dict, Iterator, List, Optional, Tuple


class BVType(enum.Enum):
    NULL = 0
    BIGINT = 1
    BOOL = 2
    BYTES = 3
    DATE = 4
    FLOAT = 5
    INTEGER = 6
    INTERVAL = 7
    JSON = 8
    DECIMAL = 9
    TEXT = 10
    TIME = 11
    TIMESTAMP = 12
    UNKNOWN = 13
    ARRAY = 14
    INTEGERARRAY = 15
    STRINGARRAY = 16


class QueryResult:
    """The BV representation of a result of a query."""

    def __init__(self):
        self.result_format = None

    def has_results(self) -> bool:
        raise NotImplementedError

    def column_count(self):
        raise NotImplementedError

    def column(self, index: int) -> Tuple[str, BVType]:
        raise NotImplementedError

    def rows(self) -> Iterator[List]:
        raise NotImplementedError

    def status(self) -> str:
        raise NotImplementedError


class Session:
    def __init__(self):
        self.id = uuid.uuid4()

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
        self._sessions[sess.id] = sess
        return sess

    def get_session(self, id: int) -> Optional[Session]:
        return self._sessions.get(id)

    def close_session(self, session: Session):
        if session and session.id in self._sessions:
            del self._sessions[session.id]
            session.close()

    def new_session(self) -> Session:
        raise NotImplementedError

    def parameters(self) -> Dict[str, str]:
        return {}


class Extension:
    @classmethod
    def check_json(cls, payload: str) -> Optional[dict]:
        is_json = False
        if payload[-1] == "}":
            is_json = True
        elif payload[-1] == ";" and payload[-2] == "}":
            is_json = True
        # Strip any SQL comments
        if is_json:
            payload = re.sub(r"\/\*.*\*\/", "", payload)
            return json.loads(payload)
        return None

    def type(self) -> str:
        raise NotImplementedError

    def apply(self, params: dict, session: Session) -> QueryResult:
        raise NotImplementedError


class SimpleQueryResult(QueryResult):
    def __init__(self, name: str, value: Any, type: BVType):
        super().__init__()
        self.name = name
        self.value = str(value)
        self.type = type

    def has_results(self):
        return True

    def column_count(self):
        return 1

    def column(self, index: int) -> Tuple[str, BVType]:
        if index == 0:
            return (self.name, self.type)
        else:
            raise IndexError

    def rows(self) -> Iterator[List]:
        return iter([[self.value]])

    def status(self) -> str:
        return ""
