import logging
import redis

import tornado.ioloop
import tornado.web

from sockjs.tornado import SockJSRouter

from .conn import Connection
from .rest import RESTRouter

from .ot import RedisTextDocumentBackend

from .auth import DummyAuthPolicy


class Application(tornado.web.Application):
    def __init__(self, *args, **kwargs):
        self.rest_router = RESTRouter("/rest")

        self.sockjs_router = SockJSRouter(Connection, "/sockjs")

        # yuck!
        self.sockjs_router.application = self

        tornado.web.Application.__init__(self,
                                         self.sockjs_router.urls +
                                         self.rest_router.urls,
                                         *args,
                                         **kwargs)

        self.redis = redis.StrictRedis(**self.settings["redis"])
        self.auth_policy = self.settings["auth_policy"](self)

        self.docs = {}
        self.conns = {}

    def get_document_backend(self, doc_id):
        return RedisTextDocumentBackend(self.redis, doc_id)


def main():
    import tornado.options
    from tornado.options import define, options

    define("config", default=None, help="config file to use")
    define("debug", default=False, help="run in debug mode", group="application")
    define("auth_policy", type=type, help="auth policy to use", group="application")
    define("port", default=9000, help="port to run on", group="application")
    define("redis", default={}, help="redis settings", group="application")

    tornado.options.parse_command_line(final=False)
    if options.config is not None:
        tornado.options.parse_config_file(options.config, final=False)
    tornado.options.parse_command_line()
    application = Application(**options.group_dict("application"))

    application.listen(options.port)
    logging.info("Starting Neuron server on port {}...".format(options.port))
    tornado.ioloop.IOLoop.instance().start()
