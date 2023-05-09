import pytest
import uuid
from typing import Iterator, List, Tuple

from buenavista.core import (
    BVType,
    QueryResult,
    Session,
    Connection,
    Extension,
    SimpleQueryResult,
)


# ----------------------- QueryResult -----------------------
class DummyQueryResult(QueryResult):
    def has_results(self) -> bool:
        return True

    def column_count(self) -> int:
        return 1

    def column(self, index: int) -> Tuple[str, BVType]:
        return ("dummy", BVType.TEXT)

    def rows(self) -> Iterator[List]:
        return iter([["dummy_row"]])

    def status(self) -> str:
        return "Dummy status"


@pytest.fixture
def dummy_query_result():
    return DummyQueryResult()


def test_query_result_has_results(dummy_query_result):
    assert dummy_query_result.has_results() is True


def test_query_result_column_count(dummy_query_result):
    assert dummy_query_result.column_count() == 1


def test_query_result_column(dummy_query_result):
    assert dummy_query_result.column(0) == ("dummy", BVType.TEXT)


def test_query_result_rows(dummy_query_result):
    assert list(dummy_query_result.rows()) == [["dummy_row"]]


def test_query_result_status(dummy_query_result):
    assert dummy_query_result.status() == "Dummy status"


# ----------------------- Session -----------------------
def test_session_init():
    session = Session()
    assert isinstance(session, Session)
    assert isinstance(session.id, uuid.UUID)


# ----------------------- Connection -----------------------
def test_connection_init():
    connection = Connection()
    assert isinstance(connection, Connection)
    assert connection._sessions == {}


# ----------------------- Extension -----------------------
def test_extension_check_json():
    payload = '{"key": "value"}'
    result = Extension.check_json(payload)
    assert result == {"key": "value"}

    payload = "not json;"
    result = Extension.check_json(payload)
    assert result is None


# ----------------------- SimpleQueryResult -----------------------
def test_simple_query_result_init():
    sqr = SimpleQueryResult("test", 42, BVType.INTEGER)
    assert sqr.name == "test"
    assert sqr.value == "42"
    assert sqr.type == BVType.INTEGER


def test_simple_query_result_has_results():
    sqr = SimpleQueryResult("test", 42, BVType.INTEGER)
    assert sqr.has_results() is True


def test_simple_query_result_column_count():
    sqr = SimpleQueryResult("test", 42, BVType.INTEGER)
    assert sqr.column_count() == 1


def test_simple_query_result_column():
    sqr = SimpleQueryResult("test", 42, BVType.INTEGER)
    assert sqr.column(0) == ("test", BVType.INTEGER)


def test_simple_query_result_rows():
    sqr = SimpleQueryResult("test", 42, BVType.INTEGER)
    assert list(sqr.rows()) == [["42"]]


def test_simple_query_result_status():
    sqr = SimpleQueryResult("test", 42, BVType.INTEGER)
    assert sqr.status() == ""
