import asyncio
import logging
import pathlib

from stream_metadata.http_api import createApplication, tcp_site
from stream_metadata.listen_websocket import listen_websocket
from stream_metadata.publish_stream3_meta import publish_stream3_meta
from stream_metadata.publish_stream_meta import publish_stream_meta

log = logging.getLogger(__name__)


# Main -------------------------------------------------------------------------

async def main(options):
    logging.basicConfig(level=options['log_level'])
    queue_meta = asyncio.Queue(maxsize=400)
    try:
        await asyncio.gather(
            listen_websocket(queue_meta, options['websocket_url']),
            publish_stream_meta(queue_meta, options['mqtt_host']),
            publish_stream3_meta(options['mqtt_host']),
            tcp_site(createApplication()),
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
