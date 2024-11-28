from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class ProxyResponse(_message.Message):
    __slots__ = ("proxy",)
    PROXY_FIELD_NUMBER: _ClassVar[int]
    proxy: str
    def __init__(self, proxy: _Optional[str] = ...) -> None: ...

class FinishProxyRequest(_message.Message):
    __slots__ = ("proxy",)
    PROXY_FIELD_NUMBER: _ClassVar[int]
    proxy: str
    def __init__(self, proxy: _Optional[str] = ...) -> None: ...

class PingResponse(_message.Message):
    __slots__ = ("response_code", "message")
    RESPONSE_CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    response_code: int
    message: str
    def __init__(self, response_code: _Optional[int] = ..., message: _Optional[str] = ...) -> None: ...
