import logging
import redis

import tornado.ioloop
import tornado.web
from tornado.options import define, options

from sockjs.tornado import SockJSRouter

from .conn import Connection
from .rest import RESTRouter

from .ot import RedisTextDocumentBackend

define("debug", default=False, help="run in debug mode")
define("port", default=8080, help="port to run on")
define("redis_host", default="localhost", help="host redis is running on")
define("redis_port", default=6379, help="port redis is running on")
define("redis_db", default=0, help="db to use on redis")


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
                      redis=redis.StrictRedis(host=options.redis_host,
                                              port=options.redis_port,
                                              db=options.redis_db),
                      debug=options.debug)
    # urk, yuck!
    router.application = app
    return app


def main():
    tornado.options.parse_command_line()
    application = make_application()

    application.listen(options.port)
    logging.info("Starting Neuron server on port {}...".format(options.port))
    tornado.ioloop.IOLoop.instance().start()
