import os
import threading

import duckdb
from fastapi import FastAPI
from pydantic import BaseModel

from buenavista.backend.duckdb import DuckDBAdapter
from buenavista.core import BuenaVistaServer
from buenavista.extensions import dbt

app = FastAPI()


@app.on_event("startup")
def startup():
    if "DUCKDB_FILE" in os.environ:
        db = duckdb.connect(os.environ["DUCKDB_FILE"])
    else:
        db = duckdb.connect()
    app.bv = BuenaVistaServer(
        ("localhost", 5433), DuckDBAdapter(db), [dbt.DbtPythonRunner()]
    )
    bv_thread = threading.Thread(target=app.bv.serve_forever)
    bv_thread.daemon = True
    bv_thread.start()


class QueryRequest(BaseModel):
    sql: str


@app.post("/query")
def query(q: QueryRequest):
    handle = app.bv.adapter.create_handle()
    query_result = handle.execute_sql(q.sql)
    res = {}
    if query_result.has_results():
        res["columns"] = [
            query_result.column(i)[0] for i in range(query_result.column_count())
        ]
        res["rows"] = list(query_result.rows())
    handle.close()
    return res
