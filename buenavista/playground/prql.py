from typing import Dict

import prql_python

from buenavista.core import Adapter, AdapterHandle, QueryResult


class PRQLAdapterHandle(AdapterHandle):
    def __init__(self, delegate: AdapterHandle):
        self.delegate = delegate
        self.process_id = self.delegate.process_id
        self.secret_key = self.delegate.secret_key

    def cursor(self):
        return self.delegate.cursor()

    def close(self):
        self.delegate.close()

    def execute_sql(self, sql: str, params=None) -> QueryResult:
        compiled_sql = prql_python.compile(sql.replace(";", ""))
        return self.delegate.execute_sql(compiled_sql, params)

    def in_transaction(self) -> bool:
        return self.delegate.in_transaction()

    def load_df_function(self, table: str):
        return self.delegate.load_df_function(table)


class PRQLAdapter(Adapter):
    def __init__(self, delegate: Adapter):
        super().__init__()
        self.delegate = delegate

    def new_handle(self) -> AdapterHandle:
        dh = self.delegate.new_handle()
        return PRQLAdapterHandle(dh)

    def parameters(self) -> Dict[str, str]:
        return self.delegate.parameters()


if __name__ == "__main__":
    import logging
    import os
    import sys

    import duckdb
    from buenavista.core import BuenaVistaServer
    from buenavista.backends.duckdb import DuckDBAdapter

    logging.basicConfig(format="%(thread)d: %(message)s", level=logging.DEBUG)

    print("Using DuckDB database at %s", sys.argv[1])
    db = duckdb.connect(sys.argv[1])

    bv_host = "0.0.0.0"
    bv_port = 5433

    if "BUENAVISTA_HOST" in os.environ:
        bv_host = os.environ["BUENAVISTA_HOST"]

    if "BUENAVISTA_PORT" in os.environ:
        bv_port = int(os.environ["BUENAVISTA_PORT"])

    address = (bv_host, bv_port)

    server = BuenaVistaServer(address, PRQLAdapter(DuckDBAdapter(db)))
    ip, port = server.server_address
    print("Listening on {ip}:{port}".format(ip=ip, port=port))

    try:
        server.serve_forever()
    finally:
        server.shutdown()
        db.close()
