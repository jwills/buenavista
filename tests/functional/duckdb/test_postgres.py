import threading

import pytest

import duckdb
import psycopg

from buenavista.examples.duckdb_postgres import create


@pytest.fixture(scope="session")
def db():
    return duckdb.connect()


@pytest.fixture(scope="session")
def user_password():
    return {"postgres": "postgres1729"}


@pytest.fixture(scope="session")
def duckdb_postgres_server(db, user_password):
    try:
        server = create(db, ("localhost", 5444), auth=user_password)
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.daemon = True
        server_thread.start()
        yield server
    finally:
        db.close()


@pytest.fixture(scope="session")
def conn(duckdb_postgres_server, user_password):
    assert duckdb_postgres_server is not None
    user, password = list(user_password.items())[0]
    conn_str = f"postgresql://{user}:{password}@localhost:5444/memory"
    return psycopg.connect(conn_str)


def test_select(conn):
    cur = conn.cursor()
    cur.execute("SELECT 1")
    assert cur.fetchone() == (1,)
    cur.close()


def test_pg_version(conn):
    cur = conn.cursor()
    cur.execute("SELECT pg_catalog.version()")
    assert cur.fetchone() == ("PostgreSQL 9.3",)
    cur.close()
