import asyncio
import concurrent.futures
import functools
import logging
import time
from typing import List, Optional

from fastapi import FastAPI, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from . import context, schemas, type_mapping
from ..core import Connection, Extension, Session, QueryResult
from ..rewrite import Rewriter

logger = logging.getLogger(__name__)


def quacko(
    app: FastAPI,
    conn: Connection,
    rewriter: Optional[Rewriter] = None,
    extensions: List[Extension] = [],
):
    pool = concurrent.futures.ThreadPoolExecutor()
    start_time = time.time()
    extensions_lookup = {e.type(): e for e in extensions}

    @app.get("/v1/info")
    async def info():
        uptime_minutes = (time.time() - start_time) / 60.0
        return {
            "coordinator": True,
            "environment": "buenavista",
            "starting": False,
            "nodeVersion": {"version": 408},
            "uptime": f"{uptime_minutes:.2f} minutes",
        }

    @app.post("/v1/statement")
    async def statement(req: Request) -> Response:
        # TODO: check user, do stuff with it
        ctxt = context.Context(conn, req)
        raw_query = await req.body()
        query = raw_query.decode("utf-8")
        logger.info("HTTP Query: %s", query)
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            pool, functools.partial(_execute, ctxt, query)
        )
        return JSONResponse(content=jsonable_encoder(result), headers=ctxt.headers())

    def _execute(ctx: context.Context, query: str) -> schemas.BaseResult:
        start = round(time.time() * 1000)
        id = f"{start_time}_{start}"
        try:
            if req := Extension.check_json(query):
                method = req.get("method")
                extension = extensions_lookup.get(method)
                if not extension:
                    raise Exception("Unknown method: " + str(method))
                else:
                    qr = extension.apply(req.get("params"), ctx.session())
            else:
                if rewriter:
                    query = rewriter.rewrite(query)
                qr = ctx.execute_sql(query)

            logger.debug(
                f"Query %s has %d columns in response", query, qr.column_count()
            )
            cols, data, update_type = _convert_query_result(qr)

            return schemas.QueryResult(
                id=id,
                info_uri="http://127.0.0.1/info",
                columns=cols,
                data=data,
                update_type=update_type,
                stats=schemas.StatementStats(
                    state="COMPLETE",
                    elapsed_time_millis=(round(time.time() * 1000) - start),
                ),
            )
        except Exception as e:
            return schemas.ErrorResult(
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
        finally:
            ctx.close()


def _convert_query_result(qr: QueryResult):
    # Special handling for DESCRIBE-style results for reasons
    if qr.column_count() == 6:
        if qr.column(0)[0] == "column_name" and qr.column(1)[0] == "column_type":
            logger.info("Performing DESCRIBE conversion on QueryResults")
            cols = type_mapping.DESCRIBE_COLUMNS
            data = []
            for r in qr.rows():
                data.append([r[0], r[1], "", ""])
            return cols, data, None

    cols, converters = [], []
    for i in range(qr.column_count()):
        name, bvtype = qr.column(i)
        ttype, cts = type_mapping.to_trino(bvtype)
        cols.append(schemas.Column(name=name, type=ttype, type_signature=cts))
        converters.append(type_mapping.type_converter(bvtype))

    data = []
    for r in qr.rows():
        data.append([converters[i](v) for i, v in enumerate(r)])
    return cols, data, None
