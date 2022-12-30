import os
import threading

import duckdb
from fastapi import FastAPI

from buenavista import core
from buenavista.backend.duckdb import DuckDBAdapter
from buenavista.extensions import dbt

app = FastAPI()


@app.on_event("startup")
def startup():
    if "DUCKDB_FILE" in os.environ:
        db = duckdb.connect(os.environ["DUCKDB_FILE"])
    else:
        db = duckdb.connect()
    app.bv = core.BuenaVistaServer(
        ("localhost", 5433), DuckDBAdapter(db), [dbt.DbtPythonRunner()]
    )
    bv_thread = threading.Thread(target=app.bv.serve_forever)
    bv_thread.daemon = True
    bv_thread.start()
