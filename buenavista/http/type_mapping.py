from typing import List, Tuple

from ..core import BVType
from .schemas import ClientTypeSignature, ClientTypeSignatureParameter


def _long(value: int) -> ClientTypeSignatureParameter:
    return ClientTypeSignatureParameter(kind="LONG", value=value)


def _cts(
    raw_type: str, arguments: List[ClientTypeSignatureParameter] = []
) -> ClientTypeSignature:
    return ClientTypeSignature(raw_type=raw_type, arguments=arguments)


TYPE_MAPPING = {
    BVType.BIGINT: ("bigint", _cts("bigint")),
    BVType.BOOL: ("bool", _cts("bool")),
    BVType.BYTES: ("bytes", _cts("bytes")),
    BVType.DATE: ("date", _cts("date")),
    BVType.FLOAT: ("real", _cts("real")),
    BVType.INTEGER: ("integer", _cts("integer")),
    # Special casing for intervals tbd
    BVType.JSON: ("json", _cts("json")),
    BVType.DECIMAL: ("decimal(38, 0)", _cts("decimal", [_long(38), _long(0)])),
    # Special casing for null tbd
    BVType.TEXT: ("varchar", _cts("varchar", [_long(2**32)])),
    BVType.TIME: ("time", _cts("time")),
    BVType.TIMESTAMP: ("timestamp", _cts("timestamp")),
}


def to_trino(bvtype: BVType) -> Tuple[str, ClientTypeSignature]:
    ret = TYPE_MAPPING.get(bvtype)
    if not ret:
        raise Exception("Trino adapter cannot handle type: " + str(bvtype))
    return ret
