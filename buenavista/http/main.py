import asyncio
import concurrent.futures
import functools
import time
from typing import List

import duckdb
from fastapi import Body, FastAPI, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from . import schemas
from ..core import Session, QueryResult
from ..backends.duckdb import DuckDBConnection

app = FastAPI()


@app.on_event("startup")
def startup():
    # TODO: generalize this config here
    app.conn = DuckDBConnection(duckdb.connect())
    app.pool = concurrent.futures.ThreadPoolExecutor()


@app.on_event("shutdown")
def shutdown():
    app.conn = None
    app.pool = None


@app.get("/v1/info")
async def info():
    return {
        "coordinator": {},
        "workers": [],
        "memory": {},
        "jvm": {},
        "system": {},
    }


@app.post("/v1/statement")
async def statement(req: Request, query: str = Body(...)) -> Response:
    # user = req.headers["X-Presto-User"]
    # Get/create handle here
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
        return schemas.QueryResults(
            id=id,
            info_uri="http://127.0.0.1/info",
            columns=_to_columns(qr),
            data=list(qr.rows()),
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


def _to_columns(qr: QueryResult) -> List[schemas.Column]:
    ret = []
    for i in range(qr.column_count()):
        col = qr.column(i)
        ret.append(schemas.Column(name=col[0], type=str(col[1])))
    return ret