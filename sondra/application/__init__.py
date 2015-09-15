from collections.abc import Mapping
from abc import ABCMeta
from copy import copy

import rethinkdb as r
import logging
import logging.config

from sondra import utils
from sondra.suite import BASIC_TYPES

from . import signals


class ApplicationException(Exception):
    """Represents a misconfiguration in an :class:`Application` class definition"""


class ApplicationMetaclass(ABCMeta):
    def __init__(cls, name, bases, nmspc):
        super(ApplicationMetaclass, cls).__init__(name, bases, nmspc)
        cls._collection_registry = {}

    def __iter__(cls):
        return (i for i in cls._collection_registry.items())

    def register_collection(cls, collection_class):
        if collection_class.name not in cls._collection_registry:
            cls._collection_registry[collection_class.slug] = collection_class
        else:
            raise ApplicationException("{0} registered twice".format(collection_class.slug))


class Application(Mapping, metaclass=ApplicationMetaclass):
    """A reusable group of :class:`Collections` and optional top-level exposed functionality.

    An Application can contain any number of :class:`Collection`s.

    """
    db = 'default'
    connection = 'default'
    slug = None
    collections = None
    anonymous_reads = True

    def __init__(self, suite, name=None):
        self.env = suite
        self.name = name or self.__class__.__name__
        self.slug = utils.camelcase_slugify(self.name)
        self.db = utils.convert_camelcase(self.name)
        self.connection = suite.connections[self.connection]
        self.collections = {}
        self.url = '/'.join((self.env.base_url, self.slug))
        self.log = logging.getLogger(self.name)
        self.application = self

        signals.pre_registration.send(self.__class__, instance=self)
        suite.register_application(self)
        signals.post_registration.send(self.__class__, instance=self)

        signals.pre_init.send(self.__class__, instance=self)
        for name, collection_class in self.__class__:
            self.collections[name] = collection_class(self)
        signals.post_init.send(self.__class__, instance=self)

    def __len__(self):
        return len(self.collections)

    def __getitem__(self, item):
        return self.collections[item]

    def __iter__(self):
        return iter(self.collections)

    def __contains__(self, item):
        return item in self.collections

    def create_tables(self, *args, **kwargs):
        signals.pre_create_tables.send(self.__class__, instance=self, args=args, kwargs=kwargs)

        for collection_class in self.collections.values():
            try:
                collection_class.create_table(*args, **kwargs)
            except Exception as e:
                self.log.warning(str(e))

        signals.post_create_tables.send(self.__class__, instance=self)

    def drop_tables(self, *args, **kwargs):
        signals.pre_delete_tables.send(self.__class__, instance=self, args=args, kwargs=kwargs)

        for collection_class in self.collections.values():
            try:
                collection_class.drop_table(*args, **kwargs)
            except Exception as e:
                self.log.warning(str(e))

        signals.post_delete_tables.send(self.__class__, instance=self)

    def create_database(self):
        try:
            r.db_create(self.db).run(self.connection)
        except r.ReqlError as e:
            self.log.warning(str(e))

    def drop_database(self):
        try:
            r.db_create(self.db).run(self.connection)
        except r.ReqlError as e:
            self.log.warning(str(e))

    @property
    def schema(self):
        ret = {
            "id": self.url + ";schema",
            "description": self.__doc__ or "No description provided",
            "definitions": copy(BASIC_TYPES)
        }

        for name, coll in self.collections.items():
            ret['definitions'][name] = coll.schema

        return ret