from typing import Dict

import psycopg2

from buenavista.adapter import *


class Psycopg2Adapter(Adapter):
    def __init__(self, **kwargs):
        self.db = psycopg2.connect(**kwargs)

    def _cursor(self):
        return self.db.cursor()

    def to_query_result(self, description, rows) -> QueryResult:
        fields = []
        for field in description:
            fields.append(Field(field.name, field.type_code))
        return QueryResult(fields, rows)

    def parameters(self) -> Dict[str, str]:
        return {
            "server_version": self.db.get_parameter_status("server_version"),
            "client_encoding": self.db.get_parameter_status("client_encoding"),
        }

    def to_query_result(self, description, rows) -> QueryResult:
        if not description:
            return QueryResult([], [])

        fields = []
        for d in description:
            name, oid = d[0], d[1]
            f = None
            if oid == BOOL_TYPE.oid:
                f = Field(name, BOOL_TYPE)
            elif oid == TEXT_TYPE.oid:
                f = Field(name, TEXT_TYPE)
            elif oid == NUMERIC_TYPE.oid:
                f = Field(name, NUMERIC_TYPE)
            elif oid == TIMESTAMP_TYPE.oid:
                f = Field(name, TIMESTAMP_TYPE)
            elif oid == INTERVAL_TYPE.oid:
                f = Field(name, INTERVAL_TYPE)
            elif oid == TIME_TYPE.oid:
                f = Field(name, TIME_TYPE)
            elif oid == DATE_TYPE.oid:
                f = Field(name, DATE_TYPE)
            elif oid == BYTES_TYPE.oid:
                f = Field(name, BYTES_TYPE)
            elif oid == INTEGER_TYPE.oid:
                f = Field(name, INTEGER_TYPE)
            else:
                f = Field(name, UNKNOWN_TYPE)
            fields.append(f)
        return QueryResult(fields, rows)


if __name__ == "__main__":
    from buenavista.core import BuenaVistaServer

    address = ("localhost", 5433)
    server = BuenaVistaServer(address, Psycopg2Adapter(database="postgres"))
    ip, port = server.server_address
    print("Listening on {ip}:{port}".format(ip=ip, port=port))
    server.serve_forever()
