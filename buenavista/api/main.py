import os
import threading

import duckdb
from fastapi import FastAPI, BackgroundTasks

from buenavista import core
from buenavista.backend.duckdb import DuckDBAdapter
from buenavista.dbt import runner as dbt_runner

app = FastAPI()


@app.on_event("startup")
def startup():
    if "DUCKDB_FILE" in os.environ:
        db = duckdb.connect(os.environ["DUCKDB_FILE"])
    else:
        db = duckdb.connect()
    app.adapter = DuckDBAdapter(db)
    app.process_status = {}
    bv = core.BuenaVistaServer(("localhost", 5433), app.adapter)
    bv_thread = threading.Thread(target=bv.serve_forever)
    bv_thread.daemon = True
    bv_thread.start()


@app.post("/submit_dbt_python_job")
def submit_dbt_python_job(
    job: dbt_runner.DbtPythonJob, background_tasks: BackgroundTasks
):
    background_tasks.add_task(
        dbt_runner.run_python_job,
        job=job,
        adapter=app.adapter,
        process_status=app.process_status,
    )
    res = {"ok": True, "status": "Submitted"}
    app.process_status[job.process_id] = res
    return res


@app.get("/check_dbt_job_status")
def check_dbt_job_status(process_id: int):
    return app.process_status.get(process_id, {"ok": False, "status": "Not Found"})
