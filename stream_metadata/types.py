import base64
import datetime
import re
from collections.abc import Mapping, Sequence
from typing import NamedTuple, Self

import msgpack

type JsonPrimitives = str | int | float | bool | None
type JsonObject = Mapping[str, Json | JsonPrimitives]
type JsonSequence = Sequence[Json | JsonPrimitives]
type Json = JsonObject | JsonSequence


class SteamMeta(NamedTuple):
    REGEX_METADATA_FIELD = re.compile(r"""(?P<key>\w+)='(?P<value>.*?)'""")

    name: str
    StreamTitle: str
    StreamUrl: str
    track_info_base64encoded: str
    UTC: datetime.datetime

    @classmethod
    def from_str(cls, name: str, data_str: str) -> Self:
        r"""
        >>> meta = SteamMeta.from_str(name='test', data_str="StreamTitle='Aaliyah - Back \u0026 Forth';StreamUrl='http://www.capitalxtra.com';track_info='k4Smc3RhdHVzoUihQNJo1pBfpHR5cGWhVKJpZKYzNjA3OTSEpnN0YXR1c6FDoUDSaNaROKR0eXBloVSiaWSmMzYwNTc4hKZzdGF0dXOhQ6FA0mjWkeKkdHlwZaFUomlkpjM2MDQ3NQ==';UTC='20250926T130915.688'")
        >>> meta
        SteamMeta(name='test', StreamTitle='Aaliyah - Back & Forth', StreamUrl='http://www.capitalxtra.com', track_info_base64encoded='k4Smc3RhdHVzoUihQNJo1pBfpHR5cGWhVKJpZKYzNjA3OTSEpnN0YXR1c6FDoUDSaNaROKR0eXBloVSiaWSmMzYwNTc4hKZzdGF0dXOhQ6FA0mjWkeKkdHlwZaFUomlkpjM2MDQ3NQ==', UTC=datetime.datetime(2025, 9, 26, 13, 9, 15, 688000))
        >>> meta.track_info_decoded
        [{'status': 'H', '@': 1758892127, 'type': 'T', 'id': '360794'}, {'status': 'C', '@': 1758892344, 'type': 'T', 'id': '360578'}, {'status': 'C', '@': 1758892514, 'type': 'T', 'id': '360475'}]
        """
        data = {
            match.group("key"): match.group("value")
            for match in cls.REGEX_METADATA_FIELD.finditer(data_str)
        }
        return cls(
            name=name,
            StreamTitle=data.get("StreamTitle", ""),
            StreamUrl=data.get("StreamUrl", ""),
            track_info_base64encoded=data.get("track_info", ""),
            UTC=datetime.datetime.strptime(
                data.get("UTC", ""), r"%Y%m%dT%H%M%S.%f"
            ),  # TODO: parse milliseconds correctly
        )

    @property
    def tack_info_msgpack_bytes(self) -> bytes:
        return base64.b64decode(self.track_info_base64encoded)

    @property
    def track_info_decoded(self) -> Json:
        return msgpack.unpackb(self.tack_info_msgpack_bytes)
