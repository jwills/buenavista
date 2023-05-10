import io
import pytest
from buenavista.postgres import BVBuffer


@pytest.fixture
def bv_buffer():
    return BVBuffer()


def test_bv_buffer_init(bv_buffer):
    assert isinstance(bv_buffer, BVBuffer)
    assert isinstance(bv_buffer.stream, io.BytesIO)


def test_bv_buffer_read_write_bytes(bv_buffer):
    data = b"test_data"
    bv_buffer.write_bytes(data)
    bv_buffer.stream.seek(0)
    assert bv_buffer.read_bytes(len(data)) == data


def test_bv_buffer_read_write_byte(bv_buffer):
    data = b"T"
    bv_buffer.write_byte(data)
    bv_buffer.stream.seek(0)
    assert bv_buffer.read_byte() == data


def test_bv_buffer_read_write_int16(bv_buffer):
    data = 12345
    bv_buffer.write_int16(data)
    bv_buffer.stream.seek(0)
    assert bv_buffer.read_int16() == data


def test_bv_buffer_read_write_uint32(bv_buffer):
    data = 12345678
    bv_buffer.write_int32(data)
    bv_buffer.stream.seek(0)
    assert bv_buffer.read_uint32() == data


def test_bv_buffer_read_write_int32(bv_buffer):
    data = -12345678
    bv_buffer.write_int32(data)
    bv_buffer.stream.seek(0)
    assert bv_buffer.read_int32() == data


def test_bv_buffer_write_string(bv_buffer):
    data = "test_string"
    bv_buffer.write_string(data)
    bv_buffer.stream.seek(0)
    assert bv_buffer.stream.read(len(data) + 1) == data.encode() + b"\x00"


def test_bv_buffer_get_value(bv_buffer):
    data = b"get_value_test"
    bv_buffer.write_bytes(data)
    assert bv_buffer.get_value() == data
