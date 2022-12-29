import os
import threading

import duckdb
from fastapi import FastAPI

from buenavista.core import BuenaVistaServer
from buenavista.backend.duckdb import DuckDBAdapter

app = FastAPI()


@app.on_event("startup")
def startup():
    app.adapter = DuckDBAdapter(duckdb.connect())
    app.bv = BuenaVistaServer(("localhost", 5433), app.adapter)
    bv_thread = threading.Thread(target=app.bv.serve_forever)
    bv_thread.daemon = True
    bv_thread.start()


@app.get("/")
def index():
    return "Hello World!"
