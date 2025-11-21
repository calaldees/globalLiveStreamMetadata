import base64
import datetime
import enum
import re
from collections.abc import Mapping, Sequence
from typing import NamedTuple, Self
from functools import cached_property
from statistics import mean

import msgpack

type JsonPrimitives = str | int | float | bool | None
type JsonObject = Mapping[str, Json | JsonPrimitives]
type JsonSequence = Sequence[Json | JsonPrimitives]
type Json = JsonObject | JsonSequence


class PlayoutItemStatus(enum.StrEnum):
    H = 'H'
    C = 'C'

    @classmethod
    def from_str(cls, s: str) -> Self:
        for v in cls:
            if v == s:
                return v
        raise ValueError()


class PlayoutItemType(enum.StrEnum):
    T = 'T'

    @classmethod
    def from_str(cls, s: str) -> Self:
        for v in cls:
            if v == s:
                return v
        raise ValueError()


class PlayoutItem(NamedTuple):
    id: str
    type: PlayoutItemType
    status: PlayoutItemStatus
    at: datetime.datetime

    @classmethod
    def from_json(cls, data: JsonObject) -> Self:
        """
        >>> PlayoutItem.from_json({"status": "H", "@": 1763735018, "type": "T", "id": "912067"})
        PlayoutItem(id='912067', type=<PlayoutItemType.T: 'T'>, status=<PlayoutItemStatus.H: 'H'>, at=datetime.datetime(2025, 11, 21, 14, 23, 38))
        """
        return cls(
            id=data["id"],
            status=PlayoutItemStatus.from_str(data["status"]),
            type=PlayoutItemType.from_str(data["type"]),
            at=datetime.datetime.fromtimestamp(data["@"]),
        )

    @property
    def json(self) -> JsonObject:
        """
        >>> data = {"status": "H", "@": 1763735018, "type": "T", "id": "912067"}
        >>> PlayoutItem.from_json(data).json == data
        True
        """
        return {
            "id": self.id,
            "status": str(self.status),
            "type": str(self.type),
            "@": int(self.at.timestamp()),
        }


class PlayoutPayload(NamedTuple):
    items: Sequence[PlayoutItem]

    @classmethod
    def from_json(cls, data: JsonSequence) -> Self:
        """
        >>> PlayoutPayload.from_json([
        ...     {"status": "C", "@": 1763735234, "type": "T", "id": "780091"},
        ...     {"status": "C", "@": 1763735454, "type": "T", "id": "5120466"},
        ... ])
        PlayoutPayload(items=(PlayoutItem(id='780091', type=<PlayoutItemType.T: 'T'>, status=<PlayoutItemStatus.C: 'C'>, at=datetime.datetime(2025, 11, 21, 14, 27, 14)), PlayoutItem(id='5120466', type=<PlayoutItemType.T: 'T'>, status=<PlayoutItemStatus.C: 'C'>, at=datetime.datetime(2025, 11, 21, 14, 30, 54))))
        """
        return cls(items=tuple(map(PlayoutItem.from_json, data)))

    @cached_property
    def ids(self) -> Sequence[str]:
        return tuple(i.id for i in self.items)

    def mean_at(self) -> float:
        return mean(i.at.timestamp() for i in self.items)

    @property
    def json(self) -> JsonSequence:
        return tuple(i.json for i in self.items)


class SteamMeta(NamedTuple):
    """
    Handling the full stream metadata string
    """

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
        >>> meta.playout_payload_json
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
    def playout_payload_msgpack_bytes(self) -> bytes:
        return base64.b64decode(self.track_info_base64encoded)

    @property
    def playout_payload_json(self) -> JsonSequence:
        return msgpack.unpackb(self.playout_payload_msgpack_bytes)

    @property
    def playout_payload(self) -> PlayoutPayload:
        return PlayoutPayload.from_json(self.playout_payload_json)
