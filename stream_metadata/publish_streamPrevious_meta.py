import asyncio
import logging
from collections.abc import Mapping

import aiomqtt
import msgpack

from .types import PlayoutPayload, StreamPlayoutPayloads

log = logging.getLogger(__name__)


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
                    if not message.payload:
                        continue
                    # Optimisation: Don't process HD or MP3 streams as these are duplicates of the core stream
                    if any(
                        message.topic.value.endswith(exclude_channel_suffix)
                        for exclude_channel_suffix in ("HD", "MP3")
                    ):
                        continue

                    log.info(f"recv: {message.topic.value}")

                    if message.topic.matches("/stream/#"):
                        # Combine and push `/streamPrevious/` version with previous payloads
                        meta_name: str = message.topic.value.removeprefix("/stream/")

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
