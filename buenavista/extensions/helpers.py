from typing import Iterator, List, Optional, Tuple

from buenavista.adapter import QueryResult
from buenavista.types import PGTypes


class SimpleQueryResult(QueryResult):
    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value

    def has_results(self):
        return True

    def column_count(self):
        return 1

    def column(self, index: int) -> Tuple[str, int]:
        if index == 0:
            return (self.name, PGTypes.TEXT.oid)
        else:
            raise IndexError

    def rows(self) -> Iterator[List[Optional[str]]]:
        return iter([self.value])
