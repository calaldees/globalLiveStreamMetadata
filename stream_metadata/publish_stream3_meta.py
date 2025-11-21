import asyncio
import logging
from collections.abc import Mapping, Sequence
from typing import Self

import aiomqtt
import msgpack

from .types import JsonSequence, PlayoutPayload

log = logging.getLogger(__name__)


class StreamPlayoutPayloads:
    payloads: Sequence[PlayoutPayload]

    def __init__(self, payloads: Sequence[PlayoutPayload] = (), retain_limit=4):
        self.payloads = payloads
        self.retain_limit = retain_limit

    @property
    def json(self) -> JsonSequence:
        return tuple(payload.json for payload in self.payloads)

    @classmethod
    def from_json(cls, data: JsonSequence) -> Self:
        return cls(tuple(map(PlayoutPayload.from_json, data)))

    def exists_in(self, p: PlayoutPayload) -> bool:
        return any(payload.ids == p.ids for payload in self.payloads)

    def changed(self, b: Self) -> bool:
        return all(self.exists_in(payload) for payload in b.payloads)

    def merge_payload(self, payload: PlayoutPayload) -> Self:
        payloads = tuple(filter(lambda p: p.ids != payload.ids or p.mean_at() > payload.mean_at(), self.payloads))
        payloads += (payload,)
        payloads = sorted(payloads, key=PlayoutPayload.mean_at)
        payloads = payloads[-self.retain_limit:]
        return self.__class__(payloads)

    def merge_payloads(self, b: Self) -> Self:
        # TODO: inefficient mess ... do better
        _return = self.__class__(self.payloads)
        for p in b.payloads:
            _return = _return.merge_payload(p)
        return _return



async def publish_stream3_meta(mqtt_host: str, reconnect_interval_seconds: int = 5) -> None:
    client = aiomqtt.Client(mqtt_host)
    last_stream3: Mapping[str, StreamPlayoutPayloads] = {}
    last_stream: Mapping[str, PlayoutPayload] = {}
    while True:  # running?
        try:
            async with client:
                await client.subscribe("/stream3/#")
                await client.subscribe("/stream/#")

                async for message in client.messages:
                    log.info(f"recv: {message.topic.value}")

                    if message.topic.matches("/stream/#"):
                        if not message.payload:
                            continue
                        meta_name = message.topic.value.removeprefix("/stream/")
                        incoming_stream_payload = PlayoutPayload.from_json(msgpack.unpackb(message.payload))
                        existing_stream3_payload = (last_stream3.get(meta_name) or StreamPlayoutPayloads())
                        merged_stream3_payload = existing_stream3_payload.merge_payload(incoming_stream_payload)

                        last_stream[meta_name] = incoming_stream_payload
                        await client.publish(
                            f"/stream3/{meta_name}",
                            msgpack.packb(merged_stream3_payload.json),
                            retain=True,
                        )

                    if message.topic.matches("/stream3/#"):
                        meta_name = message.topic.value.removeprefix("/stream3/")
                        incoming_stream3_payload = StreamPlayoutPayloads.from_json(msgpack.unpackb(message.payload))
                        existing_stream3_payload = (last_stream3.get(meta_name) or StreamPlayoutPayloads())
                        merged_stream3_payloads = existing_stream3_payload.merge_payloads(incoming_stream3_payload)

                        if merged_stream3_payloads.changed(existing_stream3_payload):
                            last_stream3[meta_name] = merged_stream3_payloads
                            await client.publish(
                                f"/stream3/{meta_name}",
                                msgpack.packb(merged_stream3_payloads.json),
                                retain=True,
                            )

        except aiomqtt.MqttError:
            log.warning(f"Connection lost to {mqtt_host=}; Reconnecting in {reconnect_interval_seconds=}")
            await asyncio.sleep(reconnect_interval_seconds)
        except (asyncio.QueueShutDown, asyncio.CancelledError):
            log.warning("TODO")
            break
