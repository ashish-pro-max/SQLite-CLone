
from .varint_parser import parse_varint
def parse_record(stream, column_count):
    _number_of_bytes_in_header = parse_varint(stream)
    serial_types = [parse_varint(stream) for i in range(column_count)]
    return [parse_column_value(stream, serial_type) for serial_type in serial_types]
def parse_column_value(stream, serial_type):
    if (serial_type >= 13) and (serial_type % 2 == 1):
        n_bytes = (serial_type - 13) // 2
        return stream.read(n_bytes)
    elif serial_type == 0:
        return None
    elif serial_type == 1:
        return int.from_bytes(stream.read(1), "big")
    elif serial_type == 2:
        return int.from_bytes(stream.read(2), "big")
    elif serial_type == 3:
        return int.from_bytes(stream.read(3), "big")
    elif serial_type == 4:
        return int.from_bytes(stream.read(4), "big")
    elif serial_type == 5:
        return int.from_bytes(stream.read(6), "big")
    elif serial_type == 6:
        return int.from_bytes(stream.read(8), "big")
    elif serial_type == 9:
        return 1
    else:
        raise Exception(f"Unhandled serial_type {serial_type}")