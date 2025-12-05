import asyncio
import logging

import aiomqtt

from .types import SteamMeta, Url

log = logging.getLogger(__name__)


async def publish_stream_meta(
    queue_meta: asyncio.Queue[SteamMeta],
    mqtt_host: str,  # Url?
    reconnect_interval_seconds: int = 5
) -> None:
    # async with aiomqtt.Client("test.mosquitto.org") as client:
    client = aiomqtt.Client(mqtt_host)
    while True:  # running?
        try:
            async with client:
                meta: SteamMeta
                while meta := await queue_meta.get():
                    await client.publish(
                        f"/stream/{meta.name}",
                        meta.playout_payload_msgpack_bytes,
                        retain=True,
                    )
                    log.info(f"publish: /stream/{meta.name}")
        except aiomqtt.MqttError:
            log.warning(
                f"Connection lost to {mqtt_host=}; Reconnecting in {reconnect_interval_seconds=}"
            )
            await asyncio.sleep(reconnect_interval_seconds)
        except (asyncio.QueueShutDown, asyncio.CancelledError):
            log.warning("TODO")
            break
