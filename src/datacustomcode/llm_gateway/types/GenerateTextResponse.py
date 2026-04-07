
from dataclasses import dataclass
from typing import Optional

import betterproto
import grpclib

from .google import protobuf



@dataclass
class GenerateTextResponse(betterproto.Message):
    version: str = betterproto.string_field(1)
    status_code: int = betterproto.uint32_field(2)
    data: protobuf.Struct = betterproto.message_field(3)
