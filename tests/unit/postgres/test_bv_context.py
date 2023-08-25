import pytest
from typing import Dict
from unittest.mock import MagicMock

from buenavista.core import Session
from buenavista.postgres import BVContext, TransactionStatus


@pytest.fixture
def mock_session():
    session = MagicMock(spec=Session)
    session.in_transaction.return_value = False
    return session


@pytest.fixture
def bv_context(mock_session):
    return BVContext(session=mock_session, rewriter=None, params={})


def test_bv_context_init(bv_context, mock_session):
    assert bv_context.session == mock_session
    assert bv_context.rewriter is None
    assert bv_context.params == {}
    assert isinstance(bv_context.process_id, int)
    assert isinstance(bv_context.secret_key, int)
    assert bv_context.stmts == {}
    assert bv_context.portals == {}
    assert bv_context.result_cache == {}
    assert bv_context.has_error is False


def test_bv_context_mark_error(bv_context):
    bv_context.mark_error()
    assert bv_context.has_error is True


def test_bv_context_transaction_status(bv_context, mock_session):
    mock_session.in_transaction.return_value = False
    assert bv_context.transaction_status() == TransactionStatus.IDLE

    bv_context.mark_error()
    mock_session.in_transaction.return_value = True
    assert bv_context.transaction_status() == TransactionStatus.IN_FAILED_TRANSACTION

    bv_context.has_error = False
    assert bv_context.transaction_status() == TransactionStatus.IN_TRANSACTION


def test_bv_context_add_close_statement(bv_context):
    name = "stmt1"
    sql = "SELECT * FROM test;"
    param_oids = []
    bv_context.add_statement(name, sql, param_oids)
    assert bv_context.stmts[name] == (sql, param_oids)

    bv_context.close_statement(name)
    assert name not in bv_context.stmts


def test_bv_context_add_close_portal(bv_context):
    portal_name = "portal1"
    stmt_name = "stmt1"
    params = {"param1": "value1"}
    result_fmt = [0]
    bv_context.add_portal(portal_name, stmt_name, params, result_fmt)
    assert bv_context.portals[portal_name] == (stmt_name, params, result_fmt)

    bv_context.close_portal(portal_name)
    assert portal_name not in bv_context.portals


def test_bv_context_flush(bv_context):
    # This method is a no-op, but including a test for completeness
    bv_context.flush()


def test_bv_context_sync(bv_context):
    bv_context.mark_error()
    bv_context.sync()
    assert bv_context.has_error is False
