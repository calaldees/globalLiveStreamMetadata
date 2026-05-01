from collections.abc import MutableMapping
import os
import asyncio
import logging
import operator

import aiohttp
import aiomqtt
import msgpack

from stream_metadata.publish_stream3_meta import StreamPlayoutPayloads

log = logging.getLogger(__name__)

LOOKUP_ENDPOINT = os.environ.get("LOOKUP_ENDPOINT", "http://localhost:8002/lookup/")

LOOKUP_CACHE: MutableMapping[int, dict] = {}  # TODO: at some point this need to expire


async def _lookup_track(http: aiohttp.ClientSession, playout_id: int) -> dict:
    if playout_id in LOOKUP_CACHE:
        return LOOKUP_CACHE.get(playout_id)
    LOOKUP_CACHE[playout_id] = _ = await (await http.get(LOOKUP_ENDPOINT + str(playout_id))).json()
    return _


async def publish_track_meta(
    mqtt_host: str,  # Url?
    reconnect_interval_seconds: int = 5,
) -> None:
    mqtt_client = aiomqtt.Client(mqtt_host)
    while True:  # running?
        try:
            async with mqtt_client, aiohttp.ClientSession() as http:
                await mqtt_client.subscribe("/stream3/#")
                async for message in mqtt_client.messages:
                    # log.info(f"recv: {message.topic.value}")
                    meta_name = message.topic.value.removeprefix("/stream3/")
                    incoming_stream3_payloads = StreamPlayoutPayloads.from_json(
                        msgpack.unpackb(message.payload)
                    )

                    # Fetch track images from cached lookup - falling though to an athena call
                    track_lookup: MutableMapping[int, dict] = {
                        int(track.get("playoutId", 0)): track
                        for track in await asyncio.gather(
                            *(
                                _lookup_track(http, track_id)
                                for track_id in frozenset(
                                    (
                                        playout_item.id_int
                                        for playout_payload in incoming_stream3_payloads.payloads
                                        for playout_item in playout_payload.items
                                    )
                                )
                            ),
                            return_exceptions=True,
                        )
                        if track and not isinstance(track, Exception)
                    }

                    # Merge sorted playout_item.json dedupled with track_lookup image
                    playout_items: MutableMapping[int, dict] = {}
                    for playout_item in sorted(
                        (
                            playout_item
                            for playout_payload in incoming_stream3_payloads.payloads
                            for playout_item in playout_payload.items
                        ),
                        key=operator.attrgetter("at"),
                        reverse=True,
                    ):
                        if playout_item.id_int not in playout_items:
                            playout_items[playout_item.id_int] = (
                                playout_item.json | track_lookup.get(playout_item.id_int, {})
                            )

                    await mqtt_client.publish(
                        f"/track/{meta_name}",
                        msgpack.packb(tuple(playout_items.values())),  # json.dumps(tracks),
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
