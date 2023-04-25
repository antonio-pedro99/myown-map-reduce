from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor
FAIL: Status
SUCCESS: Status

class NotifyMapper(_message.Message):
    __slots__ = ["input_paths", "num_reducer"]
    INPUT_PATHS_FIELD_NUMBER: _ClassVar[int]
    NUM_REDUCER_FIELD_NUMBER: _ClassVar[int]
    input_paths: _containers.RepeatedScalarFieldContainer[str]
    num_reducer: int
    def __init__(self, num_reducer: _Optional[int] = ..., input_paths: _Optional[_Iterable[str]] = ...) -> None: ...

class NotifyReducer(_message.Message):
    __slots__ = ["intermediate_paths", "my_index"]
    INTERMEDIATE_PATHS_FIELD_NUMBER: _ClassVar[int]
    MY_INDEX_FIELD_NUMBER: _ClassVar[int]
    intermediate_paths: _containers.RepeatedScalarFieldContainer[str]
    my_index: str
    def __init__(self, my_index: _Optional[str] = ..., intermediate_paths: _Optional[_Iterable[str]] = ...) -> None: ...

class Response(_message.Message):
    __slots__ = ["response"]
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    response: Status
    def __init__(self, response: _Optional[_Union[Status, str]] = ...) -> None: ...

class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
