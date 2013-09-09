import logging
import redis

import tornado.ioloop
import tornado.web

from sockjs.tornado import SockJSRouter
from beaker.session import Session, SessionObject

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
        self.auth_policy = self.settings["auth_policy"]()

        self.docs = {}
        self.conns = {}

    def get_document_backend(self, doc_id):
        return RedisTextDocumentBackend(self.redis, doc_id)


class CerebroAuthPolicy(object):
    def authenticate(self, request):
        # TODO: yeah.
        return "1"

        session = SessionObject({
            "HTTP_COOKIE": str(request.cookies)
        }, **self.settings["beaker"])

        # XXX: Corresponds to Cerebro's concept of user_id (in auth), not the
        #      Neuron user id. Hereinafter, Cerebro's user_id will be known as
        #      "name".
        name = session.get("user_id", None)

        if name is None:
            return None

        # TODO: check if the user actually exists in the db

        # The "name" needs to be stringified, as we expect string user "names"
        # in Neuron.
        return str(name)

    def authorize(self, doc_id):
        # TODO: yeah.
        return True


def main():
    import tornado.options
    from tornado.options import define, options

    define("config", default=None, help="config file to use")
    define("debug", default=False, help="run in debug mode", group="application")
    define("auth_policy", default=DummyAuthPolicy, help="auth policy to use", group="application")
    define("port", default=8080, help="port to run on", group="application")
    define("redis", default={}, help="redis settings", group="application")
    define("beaker", default={}, help="beaker settings", group="application")

    tornado.options.parse_command_line()
    if options.config is not None:
        tornado.options.parse_config_file(options.config)
    tornado.options.parse_command_line()
    application = Application(**options.group_dict("application"))

    application.listen(options.port)
    logging.info("Starting Neuron server on port {}...".format(options.port))
    tornado.ioloop.IOLoop.instance().start()
