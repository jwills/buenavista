import pytest

import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

from buenavista.backends.duckdb import DuckDBConnection
from buenavista.examples.duckdb_http import rewriter
from buenavista.http import main


@pytest.fixture(scope="session")
def db():
    return duckdb.connect()


@pytest.fixture(scope="session")
def client(db):
    app = FastAPI()
    main.quacko(app, DuckDBConnection(db), rewriter)
    return TestClient(app)


def test_info(client):
    response = client.get("/v1/info")
    assert response.status_code == 200


def test_select(client):
    response = client.post(
        "/v1/statement",
        json="SELECT 1",
        headers={"Content-Type": "application/json", "x-trino-user": "test"},
    )
    assert response.status_code == 200
