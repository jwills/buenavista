import json


class PGType:
    """Represents a PostgreSQL type and a function to convert a Python value to its wire format."""

    _instances = {}

    def __init__(self, oid, name, converter=None):
        if oid in self._instances:
            raise ValueError(f"Duplicate OID {oid} declared for PGType")
        self.oid = oid
        self.name = name
        self.converter = converter or str
        self._instances[oid] = self

    def convert(self, value) -> str:
        return self.converter(value)

    def __str__(self):
        return self.name

    @classmethod
    def find_by_oid(cls, oid: int) -> "PGType":
        return cls._instances.get(oid, PGTypes.UNKNOWN)


class PGTypes:
    BIGINT = PGType(20, "BIGINT")
    BOOL = PGType(16, "BOOL", lambda v: "true" if v else "false")
    BYTES = PGType(17, "BYTES", lambda v: "\\x" + v.hex())
    DATE = PGType(1082, "DATE", lambda v: v.isoformat())
    FLOAT = PGType(701, "FLOAT")
    INTEGER = PGType(23, "INTEGER")
    INTERVAL = PGType(
        1186,
        "INTERVAL",
        lambda v: f"{v.days} days {v.seconds} seconds {v.microseconds} microseconds",
    )
    JSON = PGType(114, "JSON", lambda v: json.dumps(v))
    DECIMAL = PGType(1700, "DECIMAL(38, 0)")
    NULL = PGType(-1, "NULL", lambda v: None)
    TEXT = PGType(25, "VARCHAR")
    TIME = PGType(1083, "TIME", lambda v: v.isoformat())
    TIMESTAMP = PGType(1114, "TIMESTAMP", lambda v: v.isoformat().replace("T", " "))
    UNKNOWN = PGType(705, "UNKNOWN")
