from dataclasses import dataclass

import betterproto

from .google import protobuf


@dataclass
class GenerateTextRequest(betterproto.Message):
    version: str = betterproto.string_field(1)
    model_name: str = betterproto.string_field(2)
    prompt: str = betterproto.string_field(3)
    localization: protobuf.Struct = betterproto.message_field(4)
    tags: protobuf.Struct = betterproto.message_field(5)



