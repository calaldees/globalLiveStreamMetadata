import asyncio
import pathlib

import aiohttp
from aiohttp import web as aiohttp_web


README = pathlib.Path('README.md').read_text()


async def hello(request: aiohttp_web.Request) -> aiohttp_web.Response:
    # return aiohttp_web.Response(text="Hello, world")
    data = {"some": "data"}
    return aiohttp_web.json_response(data)


async def readme(request: aiohttp_web.Request) -> aiohttp_web.Response:
    return aiohttp_web.Response(text=README)


def createApplication() -> aiohttp_web.Application:
    app = aiohttp_web.Application()
    app.add_routes((aiohttp_web.get("/", readme),))
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
