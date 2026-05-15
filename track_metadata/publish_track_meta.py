import asyncio
import logging
import os
from collections.abc import MutableMapping

import aiohttp
import aiomqtt
import msgpack

from stream_metadata.publish_streamPrevious_meta import StreamPlayoutPayloads

log = logging.getLogger(__name__)

LOOKUP_ENDPOINT = os.environ.get("LOOKUP_ENDPOINT", "http://localhost:8002/lookup/")

LOOKUP_CACHE: MutableMapping[int, dict] = {}  # TODO: at some point this need to expire


async def _lookup_track(http: aiohttp.ClientSession, playout_id: int) -> dict:
    if playout_id in LOOKUP_CACHE:
        return LOOKUP_CACHE.get(playout_id)
    LOOKUP_CACHE[playout_id] = _ = await (
        await http.get(LOOKUP_ENDPOINT + str(playout_id))
    ).json()
    return _


async def publish_track_meta(
    mqtt_host: str,  # Url?
    reconnect_interval_seconds: int = 5,
) -> None:
    mqtt_client = aiomqtt.Client(mqtt_host)
    while True:  # running?
        try:
            async with mqtt_client, aiohttp.ClientSession() as http:
                await mqtt_client.subscribe("/streamPrevious/#")
                async for message in mqtt_client.messages:
                    # log.info(f"recv: {message.topic.value}")
                    meta_name = message.topic.value.removeprefix("/streamPrevious/")
                    incoming_streamPrevious_payloads = StreamPlayoutPayloads.from_json(
                        msgpack.unpackb(message.payload)
                    )

                    playout_items = incoming_streamPrevious_payloads.items

                    # Fetch track images from cached lookup - falling though to an athena call
                    track_lookup: MutableMapping[int, dict] = {
                        int(track.get("playoutId", 0)): track  # type: ignore
                        for track in await asyncio.gather(
                            *(
                                _lookup_track(http, playout_item.id_int)
                                for playout_item in playout_items
                            ),
                            return_exceptions=True,
                        )
                        if track and not isinstance(track, Exception)
                    }

                    payload = {
                        'isPlayingTrack': incoming_streamPrevious_payloads.latest.isPlayingTrack,
                        # Merge playout_item.json with track images
                        'playout_items': tuple(
                            playout_item.json | track_lookup.get(playout_item.id_int, {})
                            for playout_item in playout_items
                        )
                    }

                    # TODO: consider pure json output rather tha msgpack
                    # (currently msgpack for ease of MQTTx settings)
                    await mqtt_client.publish(
                        f"/track/{meta_name}",
                        msgpack.packb(payload),
                        retain=True,
                    )
                    log.info(f"publish: /track/{meta_name}")

        except aiomqtt.MqttError:
            log.warning(
                f"Connection lost to {mqtt_host=}; Reconnecting in {reconnect_interval_seconds=}"
            )
            await asyncio.sleep(reconnect_interval_seconds)
        except (asyncio.QueueShutDown, asyncio.CancelledError):
            log.warning("TODO")
            break
        except Exception as ex:
            log.exception(f'unknown error; Reconnecting in {reconnect_interval_seconds=}')
            await asyncio.sleep(reconnect_interval_seconds)
