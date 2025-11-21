import asyncio
import logging
from collections.abc import Mapping

import aiomqtt
import msgpack

log = logging.getLogger(__name__)


async def publish_stream3_meta(
    mqtt_host: str, reconnect_interval_seconds: int = 5
) -> None:
    client = aiomqtt.Client(mqtt_host)
    stream3: Mapping[str, bytes] = {}
    while True:  # running?
        try:
            async with client:
                await client.subscribe("/stream3/#")
                await client.subscribe("/stream/#")
                async for message in client.messages:
                    log.info(f"recv: {message.topic.value}")
                    if message.topic.matches("/stream3/#"):
                        stream3[message.topic.value.removeprefix("/stream3/")] = (
                            message.payload
                        )
                    if message.topic.matches("/stream/#"):
                        if not message.payload:
                            continue
                        meta_name = message.topic.value.removeprefix("/stream/")
                        if stream3_payload := stream3.get(meta_name):
                            data = msgpack.unpackb(stream3_payload)
                            if len(data) >= 3:
                                data.pop(0)
                        else:
                            data = []
                        decoded_metadata = msgpack.unpackb(message.payload)
                        if decoded_metadata not in data:
                            data.append(decoded_metadata)
                        await client.publish(
                            f"/stream3/{meta_name}",
                            msgpack.packb(data),
                            retain=True,
                        )
        except aiomqtt.MqttError:
            log.warning(
                f"Connection lost to {mqtt_host=}; Reconnecting in {reconnect_interval_seconds=}"
            )
            await asyncio.sleep(reconnect_interval_seconds)
        except (asyncio.QueueShutDown, asyncio.CancelledError):
            log.warning("TODO")
            break
