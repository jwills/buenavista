import asyncio
import concurrent.futures
import functools
import os
import time

import duckdb
from fastapi import Body, FastAPI, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from . import schemas, type_mapping
from ..core import BVType, Session
from ..backends.duckdb import DuckDBConnection

TYPE_CONVERTERS = {
    BVType.DECIMAL: str,
}

app = FastAPI()
app.start_time = time.time()

if os.getenv("DUCKDB_FILE"):
    print("Loading DuckDB db: " + os.getenv("DUCKDB_FILE"))
    db = duckdb.connect(os.getenv("DUCKDB_FILE"))
else:
    print("Using in-memory DuckDB")
    db = duckdb.connect()

app.conn = DuckDBConnection(db)
app.pool = concurrent.futures.ThreadPoolExecutor()


@app.get("/v1/info")
async def info():
    uptime_minutes = (time.time() - app.start_time) / 60.0
    return {
        "coordinator": True,
        "environment": "buenavista",
        "starting": False,
        "nodeVersion": {"version": 408},
        "uptime": f"{uptime_minutes:.2f} minutes",
    }


@app.post("/v1/statement")
async def statement(req: Request, query: str = Body(...)) -> Response:
    # TODO: check user, do stuff with it
    _ = req.headers.get("X-Trino-User")
    sess = app.conn.create_session()
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        app.pool, functools.partial(_execute, sess, query)
    )
    app.conn.close_session(sess)
    return JSONResponse(content=jsonable_encoder(result))


def _execute(h: Session, query: bytes) -> schemas.QueryResults:
    start = round(time.time() * 1000)
    id = f"{h.process_id}-{start}"
    try:
        qr = h.execute_sql(query)
        cols, converters = [], []
        for i in range(qr.column_count()):
            name, bvtype = qr.column(i)
            ttype, cts = type_mapping.to_trino(bvtype)
            cols.append(schemas.Column(name=name, type=ttype, type_signature=cts))
            converters.append(TYPE_CONVERTERS.get(bvtype, lambda x: x))

        data = []
        for r in qr.rows():
            data.append([converters[i](v) for i, v in enumerate(r)])

        return schemas.QueryResults(
            id=id,
            info_uri="http://127.0.0.1/info",
            columns=cols,
            data=data,
            update_type=qr.status(),
            stats=schemas.StatementStats(
                state="COMPLETE",
                elapsed_time_millis=(round(time.time() * 1000) - start),
            ),
        )
    except Exception as e:
        return schemas.QueryResults(
            id=id,
            info_uri="http://127.0.0.1/info",
            error=schemas.QueryError(
                message=str(e),
                error_code=-1,
                retriable=False,
            ),
            stats=schemas.StatementStats(
                state="ERROR",
                elapsed_time_millis=(round(time.time() * 1000) - start),
            ),
        )
