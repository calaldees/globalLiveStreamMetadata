#!/usr/bin/env -S uv run --script
# /// script
# dependencies = [
#   "aiohttp",
#   "ujson",
#   "msgpack",
#   "aiomqtt",
# ]
# ///

from collections.abc import Mapping, Sequence
import datetime
from typing import NamedTuple, Self
import re
import base64
import asyncio

import ujson
import msgpack
import aiohttp
import aiomqtt

import logging
log = logging.getLogger(__name__)


type JsonPrimitives = str | int | float | bool | None
type Json = Mapping[str, Json | JsonPrimitives] | Sequence[Json | JsonPrimitives]
type JsonObject = Mapping[str, Json | JsonPrimitives]



WEBSOCKET_PARAMS = {
    'url': 'ws://10.7.116.20/metadata/',
    'origin': 'http://10.7.116.20',
}


class SteamMeta(NamedTuple):
    REGEX_METADATA_FIELD = re.compile(r"""(?P<key>\w+)='(?P<value>.*?)'""")

    name: str
    StreamTitle: str
    StreamUrl: str
    track_info_base64encoded: str
    UTC: str #datetime.datetime

    @classmethod
    def from_str(cls, name:str, data_str:str) -> Self:
        data = {
            match.group('key'): match.group('value')
            for match in cls.REGEX_METADATA_FIELD.finditer(data_str)
        }
        return cls(
            name=name,
            StreamTitle=data.get('StreamTitle', ''),
            StreamUrl=data.get('StreamUrl', ''),
            track_info_base64encoded=data.get('track_info', ''),
            UTC=data.get('UTC', ''), #datetime.datetime.strptime(r'%Y%m%dT%f'),
        )

    @property
    def tack_info_msgpack_bytes(self) -> bytes:
        return base64.b64decode(self.track_info_base64encoded)

    @property
    def track_info_decoded(self) -> Json:
        return msgpack.unpackb(self.tack_info_msgpack_bytes)

# StreamTitle='Aaliyah - Back \u0026 Forth';StreamUrl='http://www.capitalxtra.com';track_info='k4Smc3RhdHVzoUihQNJo1pBfpHR5cGWhVKJpZKYzNjA3OTSEpnN0YXR1c6FDoUDSaNaROKR0eXBloVSiaWSmMzYwNTc4hKZzdGF0dXOhQ6FA0mjWkeKkdHlwZaFUomlkpjM2MDQ3NQ==';UTC='20250926T130915.688'



async def listen_websocket(queue_meta: asyncio.Queue, url, reconnect_interval_seconds:int=5) -> None:
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                log.info(f'websocket connect {url=}')
                async with session.ws_connect(WEBSOCKET_PARAMS.pop('url'), **WEBSOCKET_PARAMS) as ws:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.ERROR:
                            break
                        data = ujson.loads(msg.data)
                        meta = SteamMeta.from_str(data['s'], data['m'])
                        queue_meta.put_nowait(meta)
                await session.close()
        except aiohttp.ClientError:
            log.warning(f"Connection lost to {url=}; Reconnecting in {reconnect_interval_seconds=}")
            await asyncio.sleep(reconnect_interval_seconds)


async def publish_mqtt(queue_meta: asyncio.Queue, mqtt_host:str, reconnect_interval_seconds:int=5) -> None:
    #async with aiomqtt.Client("test.mosquitto.org") as client:
    client = aiomqtt.Client(mqtt_host)
    while True:
        try:
            async with client:
                meta: SteamMeta
                while meta := await queue_meta.get():
                    await client.publish(
                        f'/stream/{meta.name}',
                        meta.tack_info_msgpack_bytes,
                        retain=True,
                    )
        except asyncio.QueueShutDown:
            log.warning('TODO')
        except aiomqtt.MqttError:
            log.warning(f"Connection lost to {mqtt_host=}; Reconnecting in {reconnect_interval_seconds=}")
            await asyncio.sleep(reconnect_interval_seconds)



# Main -------------------------------------------------------------------------

def get_args(argv=None):
    import argparse

    parser = argparse.ArgumentParser(
        prog=__name__,
        description="""?""",
    )

    parser.add_argument('--websocket_url', action='store', help='', default='ws://10.7.116.20/metadata/')
    parser.add_argument('--mqtt_host', action='store', help='', default='localhost')
    parser.add_argument('--log_level', action='store', type=int, help='loglevel of output to stdout', default=logging.WARNING)

    args = parser.parse_args(argv)
    return vars(args)


if __name__ == "__main__":
    options = get_args()
    logging.basicConfig(level=options['log_level'])

    loop = asyncio.get_event_loop()
    try:
        queue_meta = asyncio.Queue()
        loop.run_until_complete(asyncio.gather(
            listen_websocket(queue_meta, options['websocket_url']),
            publish_mqtt(queue_meta, options['mqtt_host']),
        ))
    except KeyboardInterrupt:
        pass
    #loop.run_until_complete(asyncio.sleep(2))  # Zero-sleep to allow underlying connections to close
    loop.close()