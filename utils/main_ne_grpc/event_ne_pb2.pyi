from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EventRequest(_message.Message):
    __slots__ = ("id", "text")
    ID_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    id: str
    text: str
    def __init__(self, id: _Optional[str] = ..., text: _Optional[str] = ...) -> None: ...

class StringList(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, items: _Optional[_Iterable[str]] = ...) -> None: ...

class EventSeriesResponse(_message.Message):
    __slots__ = ("response_code", "ne_events")
    class NeEventsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: StringList
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[StringList, _Mapping]] = ...) -> None: ...
    RESPONSE_CODE_FIELD_NUMBER: _ClassVar[int]
    NE_EVENTS_FIELD_NUMBER: _ClassVar[int]
    response_code: int
    ne_events: _containers.MessageMap[str, StringList]
    def __init__(self, response_code: _Optional[int] = ..., ne_events: _Optional[_Mapping[str, StringList]] = ...) -> None: ...

class GeneralResponse(_message.Message):
    __slots__ = ("response_code", "message")
    RESPONSE_CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    response_code: int
    message: str
    def __init__(self, response_code: _Optional[int] = ..., message: _Optional[str] = ...) -> None: ...
