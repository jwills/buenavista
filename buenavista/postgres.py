import io
import json
import logging
import os
import random
import socketserver
import struct
from typing import Dict, List, Optional

from .core import BVType, Connection, Extension, Session, QueryResult
from .rewrite import Rewriter

logger = logging.getLogger(__name__)

NULL_BYTE = b"\x00"


class ServerResponse:
    """Byte codes for server responses in the PG wire protocol."""

    AUTHENTICATION_REQUEST = b"R"
    BACKEND_KEY_DATA = b"K"
    BIND_COMPLETE = b"2"
    CLOSE_COMPLETE = b"3"
    COMMAND_COMPLETE = b"C"
    DATA_ROW = b"D"
    EMPTY_QUERY_RESPONSE = b"I"
    ERROR_RESPONSE = b"E"
    NO_DATA = b"n"
    NOTICE_RESPONSE = b"N"
    PARAMETER_DESCRIPTION = b"t"
    PARAMETER_STATUS = b"S"
    PARSE_COMPLETE = b"1"
    PORTAL_SUSPENDED = b"s"
    READY_FOR_QUERY = b"Z"
    ROW_DESCRIPTION = b"T"


class TransactionStatus:
    IDLE = b"I"
    IN_TRANSACTION = b"T"
    IN_FAILED_TRANSACTION = b"E"


# Client commands
class ClientCommand:
    """Byte codes for client commands in the PG wire protocol."""

    BIND = b"B"
    CLOSE = b"C"
    DESCRIBE = b"D"
    EXECUTE = b"E"
    FLUSH = b"H"
    QUERY = b"Q"
    PARSE = b"P"
    SYNC = b"S"
    TERMINATE = b"X"


PG_UNKNOWN = (705, str)
BVTYPE_TO_PGTYPE = {
    BVType.NULL: (-1, lambda v: None),
    BVType.ARRAY: (2277, lambda v: "{" + ",".join(v) + "}"),
    BVType.BIGINT: (20, str),
    BVType.BOOL: (16, lambda v: "true" if v else "false"),
    BVType.BYTES: (17, lambda v: "\\x" + v.hex()),
    BVType.DATE: (1082, lambda v: v.isoformat()),
    BVType.DECIMAL: (1700, str),
    BVType.FLOAT: (701, str),
    BVType.INTEGER: (23, str),
    BVType.INTEGERARRAY: (1007, lambda v: "{" + ",".join(v) + "}"),
    BVType.INTERVAL: (
        1186,
        lambda v: f"{v.days} days {v.seconds} seconds {v.microseconds} microseconds",
    ),
    BVType.JSON: (114, lambda v: json.dumps(v)),
    BVType.STRINGARRAY: (1009, lambda v: "{" + ",".join(v) + "}"),
    BVType.TEXT: (25, str),
    BVType.TIME: (1083, lambda v: v.isoformat()),
    BVType.TIMESTAMP: (1114, lambda v: v.isoformat().replace("T", " ")),
}


class BVBuffer(object):
    """A helper for reading and writing bytes in the format the PG wire protocol expects."""

    def __init__(self, stream=None):
        if not stream:
            stream = io.BytesIO()
        self.stream = stream

    def read_bytes(self, n) -> bytes:
        return self.stream.read(n)

    def read_byte(self) -> bytes:
        return self.read_bytes(1)

    def read_int16(self) -> int:
        data = self.read_bytes(2)
        return struct.unpack("!h", data)[0]

    def read_uint32(self) -> int:
        data = self.read_bytes(4)
        return struct.unpack("!I", data)[0]

    def read_int32(self) -> int:
        data = self.read_bytes(4)
        return struct.unpack("!i", data)[0]

    def write_bytes(self, value: bytes):
        self.stream.write(value)

    def write_byte(self, value):
        self.stream.write(struct.pack("!c", value))

    def write_int16(self, value: int):
        self.stream.write(struct.pack("!h", value))

    def write_int32(self, value: int):
        self.stream.write(struct.pack("!i", value))

    def write_string(self, value):
        self.stream.write(value.encode() if isinstance(value, str) else value)
        self.stream.write(b"\x00")

    def get_value(self) -> bytes:
        return self.stream.getvalue()


class BVContext:
    """Manages the state of a single connection to the server."""

    def __init__(
        self, session: Session, rewriter: Optional[Rewriter], params: Dict[str, str]
    ):
        self.session = session
        self.rewriter = rewriter
        self.params = params
        self.process_id = random.randint(0, 2**32 - 1)
        self.secret_key = random.randint(0, 2**32 - 1)
        self.stmts = {}
        self.portals = {}
        self.result_cache = {}
        self.has_error = False

    def mark_error(self):
        self.has_error = True

    def transaction_status(self):
        if self.session.in_transaction():
            if self.has_error:
                return TransactionStatus.IN_FAILED_TRANSACTION
            else:
                return TransactionStatus.IN_TRANSACTION
        return TransactionStatus.IDLE

    def execute_sql(self, sql: str, params=None) -> QueryResult:
        print("Input SQL: " + sql)
        if self.rewriter:
            sql = self.rewriter.rewrite(sql)
            print("Rewritten SQL: " + sql)
        return self.session.execute_sql(sql, params)

    def describe_portal(self, name: str) -> QueryResult:
        stmt, params = self.portals[name]
        sql = self.stmts[stmt]
        query_result = self.execute_sql(sql=sql, params=params)
        self.result_cache[name] = query_result
        return query_result

    def describe_statement(self, name: str) -> QueryResult:
        sql = self.stmts[name]
        return self.execute_sql(sql)

    def execute_portal(self, name: str) -> QueryResult:
        if name in self.result_cache:
            query_result = self.result_cache[name]
            del self.result_cache[name]
            return query_result
        else:
            stmt, params = self.portals[name]
            sql = self.stmts[stmt]
            return self.execute_sql(sql=sql, params=params)

    def add_statement(self, name: str, sql: str):
        self.stmts[name] = sql

    def close_statement(self, name: str):
        del self.stmts[name]

    def add_portal(self, name: str, stmt: str, params: Dict[str, str]):
        self.portals[name] = (stmt, params)

    def close_portal(self, name: str):
        del self.portals[name]

    def flush(self):
        pass

    def sync(self):
        if self.has_error:
            self.has_error = False


class BuenaVistaHandler(socketserver.StreamRequestHandler):
    def handle(self):
        self.r = BVBuffer(self.rfile)
        ctx = None
        try:
            ctx = self.handle_startup(self.server.conn)
            if ctx:
                self.server.ctxts[ctx.process_id] = ctx
            while ctx:
                type_code = self.r.read_byte()
                if not type_code or type_code == ClientCommand.TERMINATE:
                    # we're done
                    break

                msglen = self.r.read_uint32()
                if msglen > 4:
                    payload = self.r.read_bytes(msglen - 4)
                else:
                    payload = None

                if type_code == ClientCommand.QUERY:
                    self.handle_query(ctx, payload)
                elif type_code == ClientCommand.PARSE:
                    self.handle_parse(ctx, payload)
                elif type_code == ClientCommand.BIND:
                    self.handle_bind(ctx, payload)
                elif type_code == ClientCommand.DESCRIBE:
                    self.handle_describe(ctx, payload)
                elif type_code == ClientCommand.EXECUTE:
                    self.handle_execute(ctx, payload)
                elif type_code == ClientCommand.CLOSE:
                    self.handle_close(ctx, payload)
                elif type_code == ClientCommand.SYNC:
                    ctx.sync()
                    self.send_ready_for_query(ctx)
                elif type_code == ClientCommand.FLUSH:
                    ctx.flush()
                else:
                    raise Exception("Unknown type_code: %s" % type_code)
        except Exception as e:
            logger.exception(e)
            self.send_error(e)

        if ctx:
            self.server.conn.close_session(ctx.session)
            del self.server.ctxts[ctx.process_id]
            ctx = None

    def handle_startup(self, conn: Connection) -> BVContext:
        msglen = self.r.read_uint32() - 4
        code = self.r.read_uint32()
        if code == 80877103:  ## SSL request
            self.send_notice()
            return self.handle_startup(conn)
        elif code == 196608:  # Protocol 3.0
            msg = [
                x.decode("utf-8")
                for x in self.r.read_bytes(msglen - 4).split(NULL_BYTE)
            ]
            params = dict(zip(msg[::2], msg[1::2]))
            logger.info("Client connection params: %s", params)
            ctx = BVContext(conn.create_session(), self.server.rewriter, params)
            self.send_authentication_ok()
            self.send_parameter_status(conn.parameters())
            self.send_backend_key_data(ctx)
            self.send_ready_for_query(ctx)
            return ctx
        elif code == 80877102:  ## Cancel request
            process_id, secret_key = self.r.read_uint32(), self.r.read_uint32()
            ctx = self.server.ctxts.get(process_id)
            if ctx and ctx.secret_key == secret_key:
                self.server.conn.close_session(ctx.session)
                del self.server.ctxts[ctx.process_id]
                ctx = None
            return None
        else:
            raise Exception(f"Unsupported startup message: {code}")

    def handle_query(self, ctx: BVContext, payload: bytes):
        logger.debug("Handle query")
        decoded = payload.decode("utf-8").rstrip("\x00")
        try:
            # JSON payloads signal that we should use extensions
            if req := Extension.check_json(decoded):
                method = req.get("method")
                extension = self.server.extensions.get(method)
                if not extension:
                    raise Exception("Unknown method: " + str(method))
                else:
                    query_result = extension.apply(req.get("params"), ctx.session)
            else:
                query_result = ctx.execute_sql(decoded)
        except Exception as e:
            self.send_error(e)
            self.send_ready_for_query(ctx)
            return

        if query_result.has_results():
            self.send_row_description(query_result)
            row_count = self.send_data_rows(query_result)
            self.send_command_complete("SELECT %d\x00" % row_count)
        else:
            status = query_result.status()
            self.send_command_complete(f"{status}\x00")
        self.send_ready_for_query(ctx)

    def handle_parse(self, ctx: BVContext, payload: bytes):
        logger.debug("Handling parse")
        ba = bytearray(payload)
        stmt_idx = ba.index(NULL_BYTE)
        stmt = ba[:stmt_idx].decode("utf-8")
        query_idx = ba.index(NULL_BYTE, stmt_idx + 1)
        sql = ba[stmt_idx + 1 : query_idx].decode("utf-8")
        logger.debug("Parsed statement: %s", sql)
        ctx.add_statement(stmt, sql)
        self.send_parse_complete()

    def handle_bind(self, ctx: BVContext, payload: bytes):
        logger.debug("Handling bind")
        ba = bytearray(payload)
        portal_idx = ba.index(NULL_BYTE)
        portal = ba[:portal_idx].decode("utf-8")
        stmt_idx = ba.index(NULL_BYTE, portal_idx + 1)
        stmt = ba[portal_idx + 1 : stmt_idx].decode("utf-8")
        buf = BVBuffer(io.BytesIO(ba[stmt_idx + 1 :]))
        # First param format stuff...
        num_formats = buf.read_int16()
        formats = []
        for i in range(num_formats):
            formats.append(buf.read_int16())
        # ... then the actual param values
        num_params = buf.read_int16()
        if num_formats < num_params:
            if formats:
                formats = [formats[0]] * num_params
            else:
                formats = [0] * num_params
        params = []
        for i in range(num_params):
            nb = buf.read_int32()
            v = buf.read_bytes(nb)
            if formats[i] == 0:
                decoded = v.decode("utf-8")
                if decoded.startswith("{") and decoded.endswith("}"):
                    params.append(decoded[1:-1].split(","))
                else:
                    params.append(decoded)
            else:
                # TODO: I shouldn't be always assuming these are always
                # ints but I can live with it for now
                params.append(int.from_bytes(v, "big"))
        logger.debug("Bind params: %s", params)
        ctx.add_portal(portal, stmt, params)
        self.send_bind_complete()

    def handle_describe(self, ctx: BVContext, payload: bytes):
        logger.debug("Handling describe")
        ba = bytearray(payload)
        describe_type = ba[0]
        query_result = None
        if describe_type == ord("P"):
            portal = ba[1 : len(ba) - 1].decode("utf-8")
            try:
                query_result = ctx.describe_portal(portal)
            except Exception as e:
                self.send_error(e, ctx)
                return
        elif describe_type == ord("S"):
            stmt = ba[1 : len(ba) - 1].decode("utf-8")
            try:
                query_result = ctx.describe_statement(stmt)
            except Exception as e:
                self.send_error(e, ctx)
                return
        else:
            raise Exception(f"Unknown describe type: {describe_type}")
        if query_result.has_results():
            self.send_row_description(query_result)

    def handle_execute(self, ctx: BVContext, payload: bytes):
        logger.debug("Handling execute")
        if ctx.has_error:
            logger.info("Skipping execute due to previous error")
            return
        ba = bytearray(payload)
        portal_idx = ba.index(NULL_BYTE)
        portal = ba[:portal_idx].decode("utf-8")
        limit = struct.unpack("!i", ba[portal_idx + 1 : portal_idx + 5])[0]
        query_result = None
        try:
            query_result = ctx.execute_portal(portal)
        except Exception as e:
            self.send_error(e, ctx)
            return
        if query_result.has_results():
            row_count = self.send_data_rows(query_result, limit)
            self.send_command_complete("SELECT %d\x00" % row_count)
        else:
            status = query_result.status()
            self.send_command_complete(f"{status}\x00")

    def handle_close(self, ctx: BVContext, payload: bytes):
        logger.debug("Handling close")
        close_type = payload[0]
        if close_type == ord("S"):
            ctx.close_statement(payload[1:-1].decode("utf-8"))
        elif close_type == ord("P"):
            ctx.close_portal(payload[1:-1].decode("utf-8"))
        else:
            raise Exception(f"Unknown close type: {close_type}")
        self.send_close_complete()
        self.send_ready_for_query(ctx)

    def send_row_description(self, query_result: QueryResult):
        buf = BVBuffer()
        for i in range(query_result.column_count()):
            name, bvtype = query_result.column(i)
            oid = BVTYPE_TO_PGTYPE.get(bvtype, PG_UNKNOWN)[0]
            buf.write_string(name)
            buf.write_bytes(struct.pack("!ihihih", 0, 0, oid, 0, -1, 0))
        out = buf.get_value()
        sig = struct.pack(
            "!cih",
            ServerResponse.ROW_DESCRIPTION,
            len(out) + 6,
            query_result.column_count(),
        )
        self.wfile.write(sig + out)

    def send_data_rows(self, query_result: QueryResult, limit: int = 0) -> int:
        cnt = 0
        converters = []
        for i in range(query_result.column_count()):
            bvtype = query_result.column(i)[1]
            converters.append(BVTYPE_TO_PGTYPE.get(bvtype, PG_UNKNOWN)[1])
        for row in query_result.rows():
            buf = BVBuffer()
            for i, r in enumerate(row):
                if r is None:
                    buf.write_int32(-1)
                else:
                    v = converters[i](r).encode("utf-8")
                    buf.write_int32(len(v))
                    buf.write_bytes(v)
            out = buf.get_value()
            row_sig = struct.pack(
                "!cih",
                ServerResponse.DATA_ROW,
                len(out) + 6,
                query_result.column_count(),
            )
            self.wfile.write(row_sig + out)
            cnt += 1
            if limit > 0 and cnt >= limit:
                break
        return cnt

    def send_error(self, exception, ctx: Optional[BVContext] = None):
        estr = str(exception)
        logger.error(estr)
        buf = BVBuffer()
        buf.write_byte(b"M")
        buf.write_string(estr)
        buf.write_byte(NULL_BYTE)
        out = buf.get_value()
        err_sig = struct.pack("!ci", ServerResponse.ERROR_RESPONSE, len(out) + 4)
        self.wfile.write(err_sig + out)
        if ctx:
            ctx.mark_error()

    def send_notice(self):
        self.wfile.write(ServerResponse.NOTICE_RESPONSE)

    def send_authentication_ok(self):
        self.wfile.write(
            struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 8, 0)
        )

    def send_backend_key_data(self, ctx):
        self.wfile.write(
            struct.pack(
                "!ciII",
                ServerResponse.BACKEND_KEY_DATA,
                12,
                ctx.process_id,
                ctx.secret_key,
            )
        )

    def send_ready_for_query(self, ctx: Optional[BVContext]):
        logger.debug("Sending ready for query")
        status = ctx.transaction_status() if ctx else TransactionStatus.IDLE
        self.wfile.write(struct.pack("!cic", ServerResponse.READY_FOR_QUERY, 5, status))

    def send_parameter_status(self, params: Dict[str, str]):
        for name, value in params.items():
            buf = BVBuffer()
            buf.write_string(name)
            buf.write_string(value)
            out = buf.get_value()
            psig = struct.pack("!ci", ServerResponse.PARAMETER_STATUS, len(out) + 4)
            self.wfile.write(psig + out)

    def send_parse_complete(self):
        self.wfile.write(struct.pack("!ci", ServerResponse.PARSE_COMPLETE, 4))

    def send_bind_complete(self):
        self.wfile.write(struct.pack("!ci", ServerResponse.BIND_COMPLETE, 4))

    def send_close_complete(self):
        self.wfile.write(struct.pack("!ci", ServerResponse.CLOSE_COMPLETE, 4))

    def send_command_complete(self, tag: str):
        buf = BVBuffer()
        buf.write_bytes(
            struct.pack("!ci", ServerResponse.COMMAND_COMPLETE, len(tag) + 4)
        )
        buf.write_bytes(tag.encode())
        out = buf.get_value()
        self.wfile.write(out)


class BuenaVistaServer(socketserver.ThreadingTCPServer):
    """A Python socketserver for the Buena Vista Postgres proxy."""

    allow_reuse_address = True

    def __init__(
        self,
        server_address,
        conn: Connection,
        *,
        rewriter: Optional[Rewriter] = None,
        extensions: List[Extension] = [],
    ):
        super().__init__(server_address, BuenaVistaHandler)
        self.conn = conn
        self.rewriter = rewriter
        self.extensions = {e.type(): e for e in extensions}
        self.ctxts = {}

    def verify_request(self, request, client_address) -> bool:
        """Ensure all requests come from localhost until auth is in place"""
        return client_address[0] == "127.0.0.1" or "BUENAVISTA_HOST" in os.environ
