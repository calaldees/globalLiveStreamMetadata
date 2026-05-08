import asyncio
import logging
import pathlib
from os import environ

from stream_metadata.http_api import createApplication, serve_tcp_site
from stream_metadata.listen_websocket import listen_websocket
from stream_metadata.publish_stream_meta import publish_stream_meta
from stream_metadata.publish_streamPrevious_meta import publish_streamPrevious_meta
from stream_metadata.types import StreamMeta, Url
from track_metadata.publish_track_meta import publish_track_meta

log = logging.getLogger(__name__)


# Main -------------------------------------------------------------------------

async def main(options):
    logging.basicConfig(level=options['log_level'])
    queue_meta: asyncio.Queue[StreamMeta] = asyncio.Queue(maxsize=400)
    queue_timestamp: asyncio.Queue[StreamMeta] = asyncio.Queue(maxsize=1200)
    try:
        await asyncio.gather(
            listen_websocket(queue_meta, queue_timestamp, options['websocket_url']),
            publish_stream_meta(queue_meta, options['mqtt_host']),
            publish_streamPrevious_meta(options['mqtt_host']),
            serve_tcp_site(createApplication(queue_timestamp)),
            publish_track_meta(options['mqtt_host']),
        )
    except asyncio.CancelledError:
        log.info('Keyboard Interrupt')
        queue_meta.shutdown()
        queue_timestamp.shutdown()


# Command Line -----------------------------------------------------------------

def get_args(argv=None) -> dict:
    import argparse
    readme = pathlib.Path('README.md')
    parser = argparse.ArgumentParser(
        prog=__name__,
        description=readme.read_text() if readme.exists() else '',
    )
    parser.add_argument('--websocket_url', action='store', help='', type=Url, default=Url('ws://10.7.116.20/metadata/'))
    parser.add_argument('--mqtt_host', action='store', help='ues ENV MQTT_HOST', default=environ.get('MQTT_HOST', 'localhost'))  # TODO is this a Url?
    parser.add_argument('--log_level', action='store', type=int, help='loglevel of output to stdout', default=logging.DEBUG)
    args = parser.parse_args(argv)
    return vars(args)


if __name__ == "__main__":
    asyncio.run(main(get_args()))
