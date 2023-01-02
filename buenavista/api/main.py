import os
import threading

import duckdb
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
# Use this to serve a public/index.html
from starlette.responses import FileResponse 

from buenavista.backend.duckdb import DuckDBAdapter
from buenavista.core import BuenaVistaServer
from buenavista.extensions import dbt

app = FastAPI()
STATIC_FILE_DIR = os.path.join(os.path.dirname(__file__), "static")

app.mount(
    "/static",
    StaticFiles(directory=STATIC_FILE_DIR),
    name="static",
)


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


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_FILE_DIR, "index.html"))


class QueryRequest(BaseModel):
    sql: str

@app.post("/api/query")
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
