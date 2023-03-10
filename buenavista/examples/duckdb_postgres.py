import os
import sys

import duckdb

from ..backends.duckdb import DuckDBConnection
from .. import bv_dialects, postgres, rewrite


class DuckDBPostgresRewriter(rewrite.Rewriter):
    def rewrite(self, sql: str) -> str:
        if sql == "select pg_catalog.version()":
            return "SELECT 'PostgreSQL 9.3' as version"
        return super().rewrite(sql)


rewriter = DuckDBPostgresRewriter(bv_dialects.BVPostgres(), bv_dialects.BVDuckDB())


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Using in-memory DuckDB database")
        db = duckdb.connect()
    else:
        print("Using DuckDB database at %s" % sys.argv[1])
        db = duckdb.connect(sys.argv[1])

    bv_host = "0.0.0.0"
    bv_port = 5433

    if "BUENAVISTA_HOST" in os.environ:
        bv_host = os.environ["BUENAVISTA_HOST"]

    if "BUENAVISTA_PORT" in os.environ:
        bv_port = int(os.environ["BUENAVISTA_PORT"])

    address = (bv_host, bv_port)

    server = postgres.BuenaVistaServer(address, DuckDBConnection(db), rewriter=rewriter)
    ip, port = server.server_address
    print(f"Listening on {ip}:{port}")

    try:
        server.serve_forever()
    finally:
        server.shutdown()
        db.close()
