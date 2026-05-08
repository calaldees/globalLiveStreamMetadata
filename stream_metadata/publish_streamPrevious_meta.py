import asyncio
import logging
from collections.abc import Mapping, Sequence, Set, MutableMapping
from typing import Self
import itertools
import operator

import aiomqtt
import msgpack

from .types import JsonSequence, PlayoutPayload, PlayoutItem, PlayoutItemStatus

log = logging.getLogger(__name__)


class StreamPlayoutPayloads:
    payloads: Sequence[PlayoutPayload]

    def __init__(self, payloads: Sequence[PlayoutPayload] = (), retain_limit=5):
        self.payloads = payloads
        self.retain_limit = retain_limit

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

    def merge_payload(self, payload: PlayoutPayload) -> Self:
        """
        TODO: Really need to doctest this!!!
        """
        payloads = tuple(
            filter(
                lambda p: p.ids != payload.ids or p.mean_at() > payload.mean_at(),
                self.payloads,
            )
        )
        payloads += (payload,)
        payloads = sorted(payloads, key=PlayoutPayload.mean_at)
        payloads = payloads[-self.retain_limit :]
        return self.__class__(payloads)

    def merge_payloads(self, b: Self) -> Self:
        # TODO: inefficient mess ... do better
        _return = self.__class__(self.payloads)
        for p in b.payloads:
            _return = _return.merge_payload(p)
        return _return


async def publish_streamPrevious_meta(
    mqtt_host: str,  # Url?
    reconnect_interval_seconds: int = 5,
) -> None:
    client = aiomqtt.Client(mqtt_host)
    last_streamPrevious: Mapping[str, StreamPlayoutPayloads] = {}
    last_stream: Mapping[str, PlayoutPayload] = {}
    while True:  # running?
        try:
            async with client:
                await client.subscribe("/streamPrevious/#")
                await client.subscribe("/stream/#")

                async for message in client.messages:
                    log.info(f"recv: {message.topic.value}")

                    if message.topic.matches("/stream/#"):
                        # Combine and push `/streamPrevious/` version with previous payloads
                        if not message.payload:
                            continue
                        meta_name = message.topic.value.removeprefix("/stream/")
                        incoming_stream_payload = PlayoutPayload.from_json(
                            msgpack.unpackb(message.payload)
                        )
                        existing_streamPrevious_payloads = (
                            last_streamPrevious.get(meta_name)
                            or StreamPlayoutPayloads()
                        )
                        merged_streamPrevious_payloads = (
                            existing_streamPrevious_payloads.merge_payload(
                                incoming_stream_payload
                            )
                        )

                        # Publish
                        last_stream[meta_name] = incoming_stream_payload
                        last_streamPrevious[meta_name] = merged_streamPrevious_payloads
                        await client.publish(
                            f"/streamPrevious/{meta_name}",
                            msgpack.packb(merged_streamPrevious_payloads.json),
                            retain=True,
                        )
                        log.info(f"publish: /stream/ -> /streamPrevious/{meta_name}")

                    elif message.topic.matches("/streamPrevious/#"):
                        # Fallback for when we connect to an existing/previous session
                        # Most of the time this segment does nothing
                        # At startup we merge existing streamPrevious (could be outdated) with our current payload
                        meta_name = message.topic.value.removeprefix("/streamPrevious/")
                        incoming_streamPrevious_payloads = (
                            StreamPlayoutPayloads.from_json(
                                msgpack.unpackb(message.payload)
                            )
                        )

                        existing_streamPrevious_payloads = (
                            last_streamPrevious.get(meta_name)
                            or StreamPlayoutPayloads()
                        )
                        merged_streamPrevious_payloads = (
                            existing_streamPrevious_payloads.merge_payloads(
                                incoming_streamPrevious_payloads
                            )
                        )
                        if (
                            existing_streamPrevious_payloads.ids
                            != merged_streamPrevious_payloads.ids
                        ):
                            # Publish
                            last_streamPrevious[meta_name] = (
                                merged_streamPrevious_payloads
                            )
                            await client.publish(
                                f"/streamPrevious/{meta_name}",
                                msgpack.packb(merged_streamPrevious_payloads.json),
                                retain=True,
                            )
                            log.info(f"publish: MERGED /streamPrevious/{meta_name}")

        except aiomqtt.MqttError:
            log.warning(
                f"Connection lost to {mqtt_host=}; Reconnecting in {reconnect_interval_seconds=}"
            )
            await asyncio.sleep(reconnect_interval_seconds)
        except (asyncio.QueueShutDown, asyncio.CancelledError):
            log.warning("TODO")
            break
