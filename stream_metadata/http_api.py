import asyncio
import datetime
import pathlib
from collections.abc import Mapping

import aiohttp
from aiohttp import web as aiohttp_web

from .types import StreamMeta

README = pathlib.Path('README.md').read_text()


async def route_readme(request: aiohttp_web.Request) -> aiohttp_web.Response:
    return aiohttp_web.Response(text=README)


async def listen_to_queue_timestamps(app: aiohttp_web.Application):
    queue_timestamp: asyncio.Queue[StreamMeta] = app['queue_timestamp']
    while True:
        meta: StreamMeta
        while meta := await queue_timestamp.get():
            app['timestamps'][meta.name] = meta.UTC


async def route_timestamps(request: aiohttp_web.Request) -> aiohttp_web.Response:
    # return aiohttp_web.Response(text="Hello, world")
    #data = {"some": "data"}
    return aiohttp_web.json_response({
        k: v.isoformat()
        for k, v in request.app['timestamps'].items()
    })


def createApplication(queue_timestamp: asyncio.Queue[StreamMeta]) -> aiohttp_web.Application:
    app = aiohttp_web.Application()
    app.add_routes((aiohttp_web.get("/", route_readme),))

    timestamps_dict: Mapping[str, datetime.datetime] = {}
    app['timestamps'] = timestamps_dict
    app['queue_timestamp'] = queue_timestamp
    app.add_routes((aiohttp_web.get("/timestamps", route_timestamps),))
    # https://docs.aiohttp.org/en/stable/web_advanced.html#background-tasks
    async def background_tasks(app: aiohttp_web.Application):
        app[listen_to_queue_timestamps] = asyncio.create_task(listen_to_queue_timestamps(app))
        yield
        app[listen_to_queue_timestamps].cancel()
    app.cleanup_ctx.append(background_tasks)

    return app


async def serve_tcp_site(app: aiohttp.web.Application) -> None:
    # https://docs.aiohttp.org/en/stable/web_reference.html#running-applications
    runner = aiohttp_web.AppRunner(app)
    await runner.setup()
    site = aiohttp_web.TCPSite(runner, "0.0.0.0", 8000)
    await site.start()
    while True:
        try:
            await asyncio.sleep(3600)
            # wait for finish signal
        except Exception:
            break
    await runner.cleanup()


# aiohttp.web.run_app(app)
