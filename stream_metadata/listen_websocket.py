import asyncio
import datetime
import logging
from collections.abc import Mapping

import aiohttp
import humanize

from .types import SteamMeta, Url

log = logging.getLogger(__name__)


# TODO: should not be a constant
#WEBSOCKET_PARAMS = {
#    "url": Url("ws://10.7.116.20/metadata/"),
#    "origin": Url("http://10.7.116.20"),
#}


async def listen_websocket(
    queue_meta: asyncio.Queue[SteamMeta],
    queue_timestamp: asyncio.Queue[datetime.datetime],
    websocket_url: Url,
    reconnect_interval_seconds: int = 5
) -> None:
    start_time = datetime.datetime.now()
    bytes_received = 0
    payloads_received = 0
    previous_stream_meta_payload: Mapping[str, bytes] = dict()

    def _parse_ws_message(msg: aiohttp.WSMessage) -> SteamMeta:
        nonlocal bytes_received, payloads_received
        bytes_received += len(msg.data)
        payloads_received += 1
        data = msg.json()
        return SteamMeta.from_str(
            data["s"], data["m"]
        )  # TODO: exception here is invisible? Why?

    def _dedupe_meta(meta: SteamMeta) -> SteamMeta | None:
        meta_playout_payload_msgpack_bytes = meta.playout_payload_msgpack_bytes
        if meta_playout_payload_msgpack_bytes == previous_stream_meta_payload.get(meta.name):
            return
        previous_stream_meta_payload[meta.name] = meta_playout_payload_msgpack_bytes
        return meta


    WS_TIMEOUT = aiohttp.ClientWSTimeout(ws_receive=5, ws_close=5)
    #await asyncio.sleep(3)  # wait to allow MQTT listeners to sync/catchup before fire-hosing more
    while True:  # running?
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    timeout=WS_TIMEOUT,
                    url=websocket_url,
                    origin=f'http://{websocket_url._parts.netloc}',  # TEMP Hack because current websocket needs the origin header to accept the connection?
                ) as ws:
                    log.info(f"websocket connect {websocket_url=}")
                    try:
                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                # if msg.type == aiohttp.WSMsgType.ERROR:
                                continue
                            meta = _parse_ws_message(msg)
                            queue_timestamp.put_nowait(meta.UTC)
                            if _dedupe_meta(meta):
                                queue_meta.put_nowait(meta)
                    except asyncio.QueueShutDown:
                        await session.close()
                        return
                    except asyncio.QueueFull as ex:
                        log.warning("QueueFull - disconnecting websocket")
                        await session.close()
                    except Exception as ex:
                        log.exception(
                            "unknown exception in websocket message", exc_info=True
                        )
                await session.close()
        except asyncio.CancelledError:
            seconds_elapsed = (datetime.datetime.now() - start_time).seconds
            log.warning(
                f"QueueShutDown: received {payloads_received=} - Total {humanize.naturalsize(bytes_received)} - {humanize.naturalsize(bytes_received/seconds_elapsed)}/perSec"
            )
            break
        log.warning(
            f"Connection lost to {websocket_url=}; Reconnecting in {reconnect_interval_seconds=}"
        )
        await asyncio.sleep(reconnect_interval_seconds)
