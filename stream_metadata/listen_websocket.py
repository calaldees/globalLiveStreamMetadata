import asyncio
import datetime
import logging
from collections.abc import Mapping

import aiohttp
import aiomqtt
import humanize

from .types import SteamMeta

log = logging.getLogger(__name__)


# TODO: should not be a constant
WEBSOCKET_PARAMS = {
    "url": "ws://10.7.116.20/metadata/",
    "origin": "http://10.7.116.20",
}


async def listen_websocket(
    queue_meta: asyncio.Queue, url, reconnect_interval_seconds: int = 5
) -> None:
    start_time = datetime.datetime.now()
    bytes_received = 0
    payloads_received = 0
    previous_stream_meta_payload: Mapping[str, bytes] = dict()

    def _parse_ws_message_and_dedupe(msg: aiohttp.WSMessage) -> SteamMeta | None:
        nonlocal bytes_received, payloads_received
        bytes_received += len(msg.data)
        payloads_received += 1

        data = msg.json()
        meta = SteamMeta.from_str(
            data["s"], data["m"]
        )  # TODO: exception here is invisible? Why?
        meta_tack_info_msgpack_bytes = meta.tack_info_msgpack_bytes
        if meta_tack_info_msgpack_bytes == previous_stream_meta_payload.get(meta.name):
            return
        previous_stream_meta_payload[meta.name] = meta_tack_info_msgpack_bytes
        return meta

    WS_TIMEOUT = aiohttp.ClientWSTimeout(ws_receive=5, ws_close=5)
    await asyncio.sleep(
        3
    )  # wait to allow MQTT listeners to sync/catchup before fire-hosing more
    while True:  # running?
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    timeout=WS_TIMEOUT,
                    **WEBSOCKET_PARAMS,
                ) as ws:
                    log.info(f"websocket connect {url=}")
                    try:
                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                # if msg.type == aiohttp.WSMsgType.ERROR:
                                continue
                            if meta := _parse_ws_message_and_dedupe(msg):
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
            f"Connection lost to {url=}; Reconnecting in {reconnect_interval_seconds=}"
        )
        await asyncio.sleep(reconnect_interval_seconds)
