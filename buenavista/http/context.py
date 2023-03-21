import queue
import uuid
from collections import defaultdict
from typing import Any, Dict, Optional

from fastapi import Request

from ..core import Connection, Session, QueryResult


class Headers:
    def __init__(self, req: Request):
        self.read = req.headers
        self.write = {}

    def get(self, name: str, default: Optional[Any] = None) -> Optional[Any]:
        return self.read.get(
            f"X-Trino-{name}", self.read.get(f"X-Presto-{name}", default)
        )

    def set(self, name: str, value: Any):
        self.write[f"X-Trino-{name}"] = value
        self.write[f"X-Presto-{name}"] = value

    def clear(self, name: str):
        del self.write[f"X-Trino-{name}"]
        del self.write[f"X-Presto-{name}"]


class SessionPool:
    def __init__(self):
        self.pool = queue.SimpleQueue()
        self.txns = {}

    def acquire(self, conn: Connection, txn_id: Optional[Any] = None) -> Session:
        if txn_id in self.txns:
            return self.txns[txn_id]
        else:
            try:
                sess = self.pool.get(block=False)
            except queue.Empty:
                sess = conn.create_session()
            return sess

    def release(self, sess: Session, txn_id: Optional[Any] = None):
        if txn_id:
            self.txns[txn_id] = sess
        else:
            self.pool.put(sess)


class Context:
    POOLS = defaultdict(SessionPool)

    def __init__(self, conn: Connection, req: Request):
        self.h = Headers(req)
        self.txn_id = self.h.get("Transaction-Id")
        self.pool = self.POOLS[self.h.get("User", "default")]
        self._sess = self.pool.acquire(conn, self.txn_id)

        # Use a target catalog/schema, if specified
        use_target = None
        if catalog := self.h.get("Catalog"):
            use_target = catalog
        if schema := self.h.get("Schema"):
            if schema == "default":
                schema = "main"

            if use_target:
                use_target += f".{schema}"
            else:
                use_target = schema
        if use_target:
            self._sess.execute_sql(f"USE {use_target}")

    def execute_sql(self, sql: str) -> QueryResult:
        qr = self._sess.execute_sql(sql)
        ends_in_txn = self._sess.in_transaction()
        if not self.txn_id and ends_in_txn:
            self.txn_id = str(uuid.uuid4())
            self.h.set("Start-Transaction-Id", self.txn_id)
        elif self.txn_id and not ends_in_txn:
            self.h.set("Clear-Transaction-Id", self.txn_id)
            self.txn_id = None
        return qr

    def close(self):
        self.pool.release(self._sess, self.txn_id)
        self._sess = None

    def session(self) -> Session:
        return self._sess

    def headers(self) -> Dict:
        return self.h.write
