import pytest
from unittest.mock import MagicMock, patch

from buenavista.core import BVType, Connection, SimpleQueryResult
from buenavista.postgres import (
    BuenaVistaHandler,
    BVBuffer,
    BVContext,
    TransactionStatus,
)
from buenavista.rewrite import Rewriter

# Add any necessary setup or helper methods here, e.g., creating a test server and client


@pytest.fixture
def mock_handler():
    request, client_address = MagicMock(), MagicMock()
    server = MagicMock()
    server.conn = MagicMock(spec=Connection)
    server.rewriter = MagicMock(spec=Rewriter)
    server.ctxts = {}

    handler = BuenaVistaHandler(request, client_address, server)
    handler.r = MagicMock(spec=BVBuffer)
    handler.wfile = MagicMock()

    return handler


def test_handle_startup(mock_handler):
    mock_handler.r.read_uint32.side_effect = [8, 196608]
    mock_handler.r.read_bytes.return_value = b"user\x00test\x00database\x00testdb\x00"
    ctx = mock_handler.handle_startup(mock_handler.server.conn)
    assert isinstance(ctx, BVContext)
    assert ctx.session is not None
    assert ctx.params == {"user": "test", "database": "testdb"}


def test_handle_query(mock_handler):
    ctx = MagicMock(spec=BVContext)
    ctx.execute_sql.return_value = SimpleQueryResult("col1", 1, BVType.INTEGER)
    ctx.transaction_status.return_value = TransactionStatus.IDLE
    mock_handler.handle_query(ctx, b"SELECT 1;\x00")
    ctx.execute_sql.assert_called_once_with("SELECT 1;")


def test_handle_parse(mock_handler):
    ctx = MagicMock(spec=BVContext)
    mock_handler.handle_parse(ctx, b"stmt1\x00SELECT 1;\x00\x00\x00")
    ctx.add_statement.assert_called_once_with("stmt1", "SELECT 1;", [])


# Add more test cases for other methods in the BuenaVistaHandler class
