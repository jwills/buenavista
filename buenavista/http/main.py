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
from ..adapter import AdapterHandle, QueryResult
from ..backends.duckdb import DuckDBAdapter

app = FastAPI()


@app.on_event("startup")
def startup():
    app.adapter = DuckDBAdapter(duckdb.connect())
    app.pool = concurrent.futures.ThreadPoolExecutor()


@app.on_event("shutdown")
def shutdown():
    app.adapter = None
    app.pool = None


@app.post("/v1/statement")
async def statement(req: Request, query: str = Body(...)) -> Response:
    # user = req.headers["X-Presto-User"]
    # Get/create handle here
    handle = app.adapter.create_handle()
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        app.pool, functools.partial(_execute, handle, query)
    )
    app.adapter.close_handle(handle)
    return JSONResponse(content=jsonable_encoder(result))


def _execute(h: AdapterHandle, query: bytes) -> schemas.QueryResults:
    start = round(time.time() * 1000)
    id = f"{h.process_id}-{start}"
    print("Executing " + query)
    qr = h.execute_sql(query)
    elapsed = round(time.time() * 1000) - start
    return schemas.QueryResults(
        id=id,
        info_uri="http://127.0.0.1/info",
        columns=_to_columns(qr),
        data=list([[int(x) for x in r] for r in qr.rows()]),
        stats=schemas.StatementStats(
            state="COMPLETE",
            elapsed_time_millis=elapsed,
            root_stage=None,
            runtime_stats=None,
        ),
    )


def _to_columns(qr: QueryResult) -> List[schemas.Column]:
    ret = []
    for i in range(qr.column_count()):
        col = qr.column(i)
        ret.append(schemas.Column(name=col[0], type=str(col[1])))
    return ret
