from typing import Dict

import duckdb
from buenavista.adapter import *


class DuckDBAdapter(Adapter):
    def __init__(self, db):
        self.db = db

    def _cursor(self):
        return self.db.cursor()

    def parameters(self) -> Dict[str, str]:
        return {
            "server_version": "quackity.quack.quack",
            "client_encoding": "UTF8",
        }

    def rewrite_sql(self, sql: str) -> str:
        """Some minimalist SQL rewrites, inspired by postlite, to make DBeaver less unhappy."""
        if sql.startswith("SET "):
            return "SELECT 'SET'"
        elif "::regclass" in sql:
            # such a hack but it turns out to work fine here
            return sql.replace("::regclass", "")
        elif sql == "SHOW search_path":
            return "SELECT 'public' as search_path"
        elif sql == "SHOW TRANSACTION ISOLATION LEVEL":
            return "SELECT 'read committed' as transaction_isolation"
        return sql

    def to_query_result(self, description, rows) -> QueryResult:
        fields = []
        for i, d in enumerate(description):
            name, pytype = d[0], d[1]
            f = None
            if pytype == "bool":
                f = Field(name, BOOL_TYPE)
            elif pytype == "STRING":
                f = Field(name, TEXT_TYPE)
            elif pytype == "NUMERIC":
                vals = [r[i] for r in rows[:10]]
                if not vals:
                    f = Field(name, NUMERIC_TYPE)
                elif all(isinstance(v, int) for v in vals):
                    f = Field(name, BIGINT_TYPE)
                elif all(isinstance(v, float) for v in vals):
                    f = Field(name, FLOAT_TYPE)
                else:
                    f = Field(name, NUMERIC_TYPE)
            elif pytype == "DATETIME":
                f = Field(name, TIMESTAMP_TYPE)
            elif pytype == "TIMEDELTA":
                f = Field(name, INTERVAL_TYPE)
            elif pytype == "Time":
                f = Field(name, TIME_TYPE)
            elif pytype == "Date":
                f = Field(name, DATE_TYPE)
            elif pytype == "BINARY":
                f = Field(name, BYTES_TYPE)
            elif pytype == "NUMBER":
                f = Field(name, NUMERIC_TYPE)
            elif pytype == "dict":
                f = Field(name, JSON_TYPE)
            elif pytype == "list":
                # TODO: array types?
                f = Field(name, JSON_TYPE)
            else:
                f = Field(name, UNKNOWN_TYPE)
            fields.append(f)
        return QueryResult(fields, rows)


if __name__ == "__main__":
    from buenavista.core import BuenaVistaServer
    import sys

    if len(sys.argv) < 2:
        print("Using in-memory DuckDB database")
        db = duckdb.connect()
    else:
        print("Using DuckDB database at", sys.argv[1])
        db = duckdb.connect(sys.argv[1])

    # some tables/view definitions we need for DBeaver
    db.execute(
        "CREATE TABLE IF NOT EXISTS pg_catalog.pg_database AS SELECT 12345 as oid, 'main' as datname"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS pg_catalog.pg_proc (oid INTEGER, proname TEXT, pronamespace TEXT)"
    )
    db.execute(
        "CREATE OR REPLACE VIEW pg_catalog.pg_settings AS SELECT * FROM duckdb_settings()"
    )

    address = ("localhost", 5433)
    server = BuenaVistaServer(address, DuckDBAdapter(db))
    ip, port = server.server_address
    print("Listening on {ip}:{port}".format(ip=ip, port=port))
    try:
        server.serve_forever()
    finally:
        server.shutdown()
        db.close()
