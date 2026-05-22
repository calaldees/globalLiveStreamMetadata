import base64
import datetime
import enum
import itertools
import operator
import re
import urllib.parse
from collections.abc import Mapping, MutableMapping, Sequence, Set
from statistics import mean
from typing import NamedTuple, Self, TypedDict

import msgpack

type JsonPrimitives = str | int | float | bool | None
type JsonObject = Mapping[str, Json | JsonPrimitives]
type JsonSequence = Sequence[Json | JsonPrimitives]
type Json = JsonObject | JsonSequence


class Url(str):
    def __init__(self, url: str):
        self._parts = urllib.parse.urlparse(url)
        if bool(self) and not self._parts.netloc:
            raise ValueError(
                f"{self.__class__.__name__}: unable to parse {self._parts=}"
            )

    def __str__(self) -> str:
        return urllib.parse.urlunparse(self._parts)

    def __bool__(self) -> bool:
        return any(self._parts)


class PlayoutItemStatus(enum.StrEnum):
    H = "H"
    C = "C"

    @classmethod
    def from_str(cls, s: str) -> Self:
        for v in cls:
            if v == s:
                return v
        raise ValueError()


class PlayoutItemType(enum.StrEnum):
    T = "T"

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

    PlayoutItemJson = TypedDict(
        "PlayoutItemJson", {"id": str, "type": str, "status": str, "@": int}
    )

    @property
    def id_int(self) -> int:
        return int(re.sub(r"\D", "", self.id) or 0)

    @classmethod
    def from_json(cls, data: PlayoutItemJson) -> Self:
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
    def json(self) -> PlayoutItemJson:
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

    @property
    def ids(self) -> Sequence[int]:
        return tuple(i.id_int for i in self.items)

    def mean_at(self) -> float:
        return mean(i.at.timestamp() for i in self.items) if self.items else 0

    @property
    def json(self) -> JsonSequence:
        return tuple(i.json for i in self.items)

    @property
    def isPlayingTrack(self) -> bool:
        return any(
            playout_item.status == PlayoutItemStatus.H
            for playout_item in self.items
        )

    @property
    def latest_timestamp(self) -> datetime.datetime:
        return max(i.at for i in self.items) if self.items else datetime.datetime.fromtimestamp(0)


class StreamMeta(NamedTuple):
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
        >>> meta = StreamMeta.from_str(name='test', data_str="StreamTitle='Aaliyah - Back \u0026 Forth';StreamUrl='http://www.capitalxtra.com';track_info='k4Smc3RhdHVzoUihQNJo1pBfpHR5cGWhVKJpZKYzNjA3OTSEpnN0YXR1c6FDoUDSaNaROKR0eXBloVSiaWSmMzYwNTc4hKZzdGF0dXOhQ6FA0mjWkeKkdHlwZaFUomlkpjM2MDQ3NQ==';UTC='20250926T130915.688'")
        >>> meta
        StreamMeta(name='test', StreamTitle='Aaliyah - Back & Forth', StreamUrl='http://www.capitalxtra.com', track_info_base64encoded='k4Smc3RhdHVzoUihQNJo1pBfpHR5cGWhVKJpZKYzNjA3OTSEpnN0YXR1c6FDoUDSaNaROKR0eXBloVSiaWSmMzYwNTc4hKZzdGF0dXOhQ6FA0mjWkeKkdHlwZaFUomlkpjM2MDQ3NQ==', UTC=datetime.datetime(2025, 9, 26, 13, 9, 15, 688000))
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


class StreamPlayoutPayloads:
    payloads: Sequence[PlayoutPayload]

    def __init__(
        self,
        payloads: Sequence[PlayoutPayload] = (),
        retain_period: datetime.timedelta = datetime.timedelta(minutes=30),
    ):
        self.payloads = payloads
        self.retain_period = retain_period

    def __str__(self) -> str:
        return f'{self.__class__.__name__}: [{" ".join(",".join(p.ids) for p in self.payloads)}]'

    @property
    def json(self) -> JsonSequence:
        return tuple(payload.json for payload in self.payloads)

    @classmethod
    def from_json(cls, data: JsonSequence) -> Self:
        return cls(tuple(map(PlayoutPayload.from_json, data)))

    @property
    def ids(self) -> Set[str]:
        return frozenset(
            itertools.chain.from_iterable(payload.ids for payload in self.payloads)
        )

    @property
    def latest(self) -> PlayoutPayload:
        return self.payloads[-1]

    @property
    def items(self) -> Sequence[PlayoutItem]:
        playout_items: MutableMapping[int, PlayoutItem] = {}
        for playout_payload in self.payloads:
            for playout_item in playout_payload.items:
                existing_playout_item = playout_items.get(playout_item.id_int)
                if playout_item.status == PlayoutItemStatus.H or (
                    not existing_playout_item
                    or existing_playout_item.at > playout_item.at
                ):
                    playout_items[playout_item.id_int] = playout_item
        return sorted(playout_items.values(), key=operator.attrgetter("at"))

    def merge_payload(self, new_payload: PlayoutPayload) -> Self:
        """
        TODO: Really need to doctest this!!!
        """
        payloads = tuple(
            filter(
                # (True == Keep) Keep existing payloads that:
                #  - DONT contain the same tracks as the new_payload OR are newer than the new_payload
                lambda p: p.ids != new_payload.ids
                or p.mean_at() > new_payload.mean_at(),
                self.payloads,
            )
        )
        payloads += (new_payload,)
        discard_threshold_timestamp = (
            max(payload.latest_timestamp for payload in payloads) - self.retain_period
        )
        return self.__class__(
            tuple(
                filter(
                    lambda payload: payload.latest_timestamp
                    > discard_threshold_timestamp,
                    sorted(payloads, key=PlayoutPayload.mean_at),
                )
            )
        )

    def merge_payloads(self, b: Self) -> Self:
        # TODO: inefficient mess ... do better
        _return = self.__class__(self.payloads)
        for p in b.payloads:
            _return = _return.merge_payload(p)
        return _return
