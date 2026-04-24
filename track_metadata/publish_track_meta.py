from collections.abc import MutableMapping
import os
import asyncio
import logging
import json
import operator

import aiohttp
import aiomqtt
import msgpack

from stream_metadata.publish_stream3_meta import StreamPlayoutPayloads

log = logging.getLogger(__name__)

LOOKUP_ENDPOINT = os.environ.get('LOOKUP_ENDPOINT', 'http://localhost:8002/lookup/')

LOOKUP_CACHE: MutableMapping[int, dict] = {}

async def _lookup_track(http: aiohttp.ClientSession, id:int) -> dict:
    #if id not in (255519,):
    #    return {'id': id}
    if id in LOOKUP_CACHE:
        return LOOKUP_CACHE.get(id)
    LOOKUP_CACHE[id] = _ = await (await http.get(LOOKUP_ENDPOINT + str(id))).json()
    return _


async def publish_track_meta(
    mqtt_host: str,  # Url?
    reconnect_interval_seconds: int = 5
) -> None:
    mqtt_client = aiomqtt.Client(mqtt_host)
    while True:  # running?
        try:
            async with mqtt_client, aiohttp.ClientSession() as http:
                await mqtt_client.subscribe("/stream3/#")
                async for message in mqtt_client.messages:
                    # log.info(f"recv: {message.topic.value}")
                    meta_name = message.topic.value.removeprefix("/stream3/")
                    incoming_stream3_payloads = StreamPlayoutPayloads.from_json(msgpack.unpackb(message.payload))

                    track_ids = tuple(dict.fromkeys((
                        playout_item.id_int
                        for playout_item in sorted((
                            playout_item
                            for playout_payload in incoming_stream3_payloads.payloads
                            for playout_item in playout_payload.items
                        ), key=operator.attrgetter('at'))
                    )))
                    tracks_data = await asyncio.gather(*(_lookup_track(http, track_id) for track_id in track_ids))

                    await mqtt_client.publish(
                        f"/track/{meta_name}",
                        #json.dumps(tracks),
                        msgpack.packb(tracks_data),
                        retain=True,
                    )
                    log.info(f"publish: /track/{meta_name}")



        except aiomqtt.MqttError:
            log.warning(f"Connection lost to {mqtt_host=}; Reconnecting in {reconnect_interval_seconds=}")
            await asyncio.sleep(reconnect_interval_seconds)
        except (asyncio.QueueShutDown, asyncio.CancelledError):
            log.warning("TODO")
            break
