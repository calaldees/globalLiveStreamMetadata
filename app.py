from collections.abc import Mapping, Sequence
import datetime
from typing import NamedTuple, Self
import re
import base64
import asyncio
import pathlib

import humanize
import msgpack
import aiohttp
import aiomqtt

import logging
log = logging.getLogger(__name__)


type JsonPrimitives = str | int | float | bool | None
type JsonObject = Mapping[str, Json | JsonPrimitives]
type JsonSequence = Sequence[Json | JsonPrimitives]
type Json = JsonObject | JsonSequence


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
    UTC: datetime.datetime

    @classmethod
    def from_str(cls, name:str, data_str:str) -> Self:
        r"""
        >>> SteamMeta.from_str("StreamTitle='Aaliyah - Back \u0026 Forth';StreamUrl='http://www.capitalxtra.com';track_info='k4Smc3RhdHVzoUihQNJo1pBfpHR5cGWhVKJpZKYzNjA3OTSEpnN0YXR1c6FDoUDSaNaROKR0eXBloVSiaWSmMzYwNTc4hKZzdGF0dXOhQ6FA0mjWkeKkdHlwZaFUomlkpjM2MDQ3NQ==';UTC='20250926T130915.688'"
        'TODO'
        """
        data = {
            match.group('key'): match.group('value')
            for match in cls.REGEX_METADATA_FIELD.finditer(data_str)
        }
        return cls(
            name=name,
            StreamTitle=data.get('StreamTitle', ''),
            StreamUrl=data.get('StreamUrl', ''),
            track_info_base64encoded=data.get('track_info', ''),
            UTC=datetime.datetime.strptime(data.get('UTC', ''), r'%Y%m%dT%H%M%S.%f')  # TODO: parse milliseconds correctly
        )

    @property
    def tack_info_msgpack_bytes(self) -> bytes:
        return base64.b64decode(self.track_info_base64encoded)

    @property
    def track_info_decoded(self) -> Json:
        return msgpack.unpackb(self.tack_info_msgpack_bytes)


async def listen_websocket(queue_meta: asyncio.Queue, url, reconnect_interval_seconds:int=5) -> None:
    start_time = datetime.datetime.now()
    bytes_received = 0
    payloads_received = 0
    previous_stream_meta_payload: Mapping[str, bytes] = dict()

    def _parse_ws_message_and_dedupe(msg: aiohttp.WSMessage) -> SteamMeta | None:
        nonlocal bytes_received, payloads_received
        bytes_received += len(msg.data)
        payloads_received += 1

        data = msg.json()
        meta = SteamMeta.from_str(data['s'], data['m'])  # TODO: exception here is invisible? Why?
        meta_tack_info_msgpack_bytes = meta.tack_info_msgpack_bytes
        if meta_tack_info_msgpack_bytes == previous_stream_meta_payload.get(meta.name):
            return
        previous_stream_meta_payload[meta.name] = meta_tack_info_msgpack_bytes
        return meta

    WS_TIMEOUT = aiohttp.ClientWSTimeout(ws_receive=5, ws_close=5)
    while True:  # running?
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    WEBSOCKET_PARAMS.pop('url'),
                    timeout=WS_TIMEOUT,
                    **WEBSOCKET_PARAMS,
                ) as ws:
                    log.info(f'websocket connect {url=}')
                    try:
                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT:
                            #if msg.type == aiohttp.WSMsgType.ERROR:
                                break  #? #continue  # ?
                            log.info('raising!')
                            raise asyncio.QueueFull()
                            log.info('raiseed')
                            if meta := _parse_ws_message_and_dedupe(msg):
                                queue_meta.put_nowait(meta)
                    except asyncio.QueueShutDown:
                        return
                    except asyncio.QueueFull as ex:
                        log.warning('QueueFull - disconnecting websocket')
                await session.close()
                log.info('1')
        #except aiohttp.ClientError:
        #except asyncio.QueueFull:  # Why is this never called?
        #    log.warning(f"QueueFull; pausing websocket for {reconnect_interval_seconds=}")
        #    await asyncio.sleep(reconnect_interval_seconds)
        except asyncio.CancelledError:
            seconds_elapsed = (datetime.datetime.now()-start_time).seconds
            log.warning(f'QueueShutDown: received {payloads_received=} - Total {humanize.naturalsize(bytes_received)} - {humanize.naturalsize(bytes_received/seconds_elapsed)}/perSec')
            break
        log.warning(f"Connection lost to {url=}; Reconnecting in {reconnect_interval_seconds=}")
        await asyncio.sleep(reconnect_interval_seconds)



async def publish_mqtt(queue_meta: asyncio.Queue, mqtt_host:str, reconnect_interval_seconds:int=5) -> None:
    #async with aiomqtt.Client("test.mosquitto.org") as client:
    client = aiomqtt.Client(mqtt_host)
    while True:  # running?
        try:
            async with client:
                meta: SteamMeta
                while meta := await queue_meta.get():
                    await client.publish(
                        f'/stream/{meta.name}',
                        meta.tack_info_msgpack_bytes,
                        retain=True,
                    )
        except aiomqtt.MqttError:
            log.warning(f"Connection lost to {mqtt_host=}; Reconnecting in {reconnect_interval_seconds=}")
            await asyncio.sleep(reconnect_interval_seconds)
        except (asyncio.QueueShutDown, asyncio.CancelledError):
            log.warning('TODO')
            break



# Main -------------------------------------------------------------------------

async def main(options):
    logging.basicConfig(level=options['log_level'])
    queue_meta = asyncio.Queue(maxsize=1000)
    try:
        await asyncio.gather(
            listen_websocket(queue_meta, options['websocket_url']),
            publish_mqtt(queue_meta, options['mqtt_host']),
        )
    except asyncio.CancelledError:
        log.info('Keyboard Interrupt')
        queue_meta.shutdown()


# Command Line -----------------------------------------------------------------

def get_args(argv=None) -> dict:
    import argparse
    readme = pathlib.Path('README.md')
    parser = argparse.ArgumentParser(
        prog=__name__,
        description=readme.read_text() if readme.exists() else '',
    )
    parser.add_argument('--websocket_url', action='store', help='', default='ws://10.7.116.20/metadata/')
    parser.add_argument('--mqtt_host', action='store', help='', default='localhost')
    parser.add_argument('--log_level', action='store', type=int, help='loglevel of output to stdout', default=logging.DEBUG)
    args = parser.parse_args(argv)
    return vars(args)


if __name__ == "__main__":
    asyncio.run(main(get_args()))
