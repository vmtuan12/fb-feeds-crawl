# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: event_ne.proto
# Protobuf Python Version: 5.27.2
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    27,
    2,
    '',
    'event_ne.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0e\x65vent_ne.proto\x12\x08\x63onsumer\x1a\x1bgoogle/protobuf/empty.proto\"(\n\x0c\x45ventRequest\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04text\x18\x02 \x01(\t\"\x1b\n\nStringList\x12\r\n\x05items\x18\x01 \x03(\t\"\xb3\x01\n\x13\x45ventSeriesResponse\x12\x15\n\rresponse_code\x18\x01 \x01(\x05\x12>\n\tne_events\x18\x02 \x03(\x0b\x32+.consumer.EventSeriesResponse.NeEventsEntry\x1a\x45\n\rNeEventsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12#\n\x05value\x18\x02 \x01(\x0b\x32\x14.consumer.StringList:\x02\x38\x01\"9\n\x0fGeneralResponse\x12\x15\n\rresponse_code\x18\x01 \x01(\x05\x12\x0f\n\x07message\x18\x02 \x01(\t2\xd1\x01\n\x08\x43onsumer\x12\x41\n\x0cProcessEvent\x12\x16.consumer.EventRequest\x1a\x19.consumer.GeneralResponse\x12G\n\x0eGetEventSeries\x12\x16.consumer.EventRequest\x1a\x1d.consumer.EventSeriesResponse\x12\x39\n\x04Ping\x12\x16.google.protobuf.Empty\x1a\x19.consumer.GeneralResponseb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'event_ne_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_EVENTSERIESRESPONSE_NEEVENTSENTRY']._loaded_options = None
  _globals['_EVENTSERIESRESPONSE_NEEVENTSENTRY']._serialized_options = b'8\001'
  _globals['_EVENTREQUEST']._serialized_start=57
  _globals['_EVENTREQUEST']._serialized_end=97
  _globals['_STRINGLIST']._serialized_start=99
  _globals['_STRINGLIST']._serialized_end=126
  _globals['_EVENTSERIESRESPONSE']._serialized_start=129
  _globals['_EVENTSERIESRESPONSE']._serialized_end=308
  _globals['_EVENTSERIESRESPONSE_NEEVENTSENTRY']._serialized_start=239
  _globals['_EVENTSERIESRESPONSE_NEEVENTSENTRY']._serialized_end=308
  _globals['_GENERALRESPONSE']._serialized_start=310
  _globals['_GENERALRESPONSE']._serialized_end=367
  _globals['_CONSUMER']._serialized_start=370
  _globals['_CONSUMER']._serialized_end=579
# @@protoc_insertion_point(module_scope)
