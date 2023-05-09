import pytest
import sqlglot
import sqlglot.expressions as exp
from buenavista.rewrite import Rewriter

@pytest.fixture
def rewriter():
    read_dialect = sqlglot.Dialect()
    write_dialect = sqlglot.Dialect()
    return Rewriter(read=read_dialect, write=write_dialect)

def test_rewriter_initialization(rewriter):
    assert isinstance(rewriter, Rewriter)

def test_relation_decorator(rewriter):
    @rewriter.relation("test_relation")
    def test_func():
        return "SELECT * FROM test_relation"

    assert "test_relation" in rewriter._relations
    assert rewriter._relations["test_relation"]() == "SELECT * FROM test_relation"

def test_rewrite(rewriter):
    @rewriter.relation("test_relation")
    def test_func():
        return "SELECT * FROM test_relation"

    original_sql = "SELECT * FROM test_relation"
    rewritten_sql = rewriter.rewrite(original_sql)
    expected_sql = "SELECT * FROM (SELECT * FROM test_relation) /* source: test_relation */"

    assert rewritten_sql == expected_sql

def test_rewrite_with_exception(rewriter):
    faulty_sql = "SELECT * FRO test_relation"  # Typo in SQL
    rewritten_sql = rewriter.rewrite(faulty_sql)
    assert rewritten_sql == faulty_sql
