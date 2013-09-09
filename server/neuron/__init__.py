import logging
import redis

import tornado.ioloop
import tornado.web
from tornado.options import define, options

from sockjs.tornado import SockJSRouter

from .conn import Connection
from .rest import RESTRouter

from .ot import RedisTextDocumentBackend

define("config", default=None, help="config file to use")
define("debug", default=False, help="run in debug mode")
define("port", default=8080, help="port to run on")
define("redis", default={}, help="redis settings")
define("beaker", default={}, help="beaker settings")


class Application(tornado.web.Application):
    def __init__(self, *args, **kwargs):
        tornado.web.Application.__init__(self, *args, **kwargs)
        self.redis = self.settings["redis"]
        self.docs = {}
        self.conns = {}

    def get_document_backend(self, doc_id):
        return RedisTextDocumentBackend(self.redis, doc_id)


def make_application():
    router = SockJSRouter(Connection, "/sockjs")
    app = Application(RESTRouter("/rest").urls + router.urls,
                      redis=redis.StrictRedis(**options.redis),
                      debug=options.debug,
                      beaker=options.beaker)
    # urk, yuck!
    router.application = app
    return app


def main():
    tornado.options.parse_command_line()
    if options.config is not None:
        tornado.options.parse_config_file(options.config)
    tornado.options.parse_command_line()
    application = make_application()

    application.listen(options.port)
    logging.info("Starting Neuron server on port {}...".format(options.port))
    tornado.ioloop.IOLoop.instance().start()
