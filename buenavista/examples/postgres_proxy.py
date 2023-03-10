import os

from buenavista.postgres import BuenaVistaServer
from buenavista.backends.postgres import PGConnection

address = ("localhost", 5433)
server = BuenaVistaServer(
    address,
    PGConnection(
        conninfo="",
        host="localhost",
        port=5432,
        user=os.getenv("USER"),
        dbname="postgres",
    ),
)
ip, port = server.server_address
print(f"Listening on {ip}:{port}")
server.serve_forever()
