import asyncio

import aiohttp
from aiohttp import web as aiohttp_web


async def hello(request) -> aiohttp_web.Response:
    # return aiohttp_web.Response(text="Hello, world")
    data = {"some": "data"}
    return aiohttp_web.json_response(data)


def createApplication() -> aiohttp_web.Application:
    app = aiohttp_web.Application()
    app.add_routes((aiohttp_web.get("/", hello),))
    return app


async def tcp_site(app: aiohttp.web.Application) -> None:
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
