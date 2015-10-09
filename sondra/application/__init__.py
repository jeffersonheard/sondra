from blinker import signal
from collections.abc import Mapping
from abc import ABCMeta

import rethinkdb as r
import logging
import logging.config


from sondra import help, utils
from . import signals


class ApplicationException(Exception):
    """Represents a misconfiguration in an :class:`Application` class definition"""


class ApplicationMetaclass(ABCMeta):
    """Inherit definitions from base classes, and let the subclass override any definitions from the base classes."""

    def __new__(mcs, name, bases, attrs):
        definitions = {}
        for base in bases:
            if hasattr(base, "definitions") and base.definitions:
                definitions.update(base.definitions)

        if "definitions" in attrs and attrs["definitions"] is not None:
            attrs['definitions'].update(definitions)
        else:
            attrs['definitions'] = definitions

        return super().__new__(mcs, name, bases, attrs)

    def __init__(cls, name, bases, attrs):
        super(ApplicationMetaclass, cls).__init__(name, bases, attrs)
        cls.exposed_methods = {}
        for base in bases:
            if hasattr(base, 'exposed_methods'):
                cls.exposed_methods.update(base.exposed_methods)
        for name, method in (n for n in nmspc.items() if hasattr(n[1], 'exposed')):
                cls.exposed_methods[name] = method

        cls._collection_registry = {}

    def __iter__(cls):
        return (i for i in cls._collection_registry.items())

    def register_collection(cls, collection_class):
        if collection_class.slug not in cls._collection_registry:
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
    definitions = None

    @property
    def url(self):
        if self._url:
            return self._url
        elif self.suite:
            return self.suite.url + "/" + self.slug
        else:
            return self.slug

    @property
    def schema_url(self):
        return self.url + ";schema"

    @property
    def schema(self):
        return {
            "id": self.url + ";schema",
            "type": "object",
            "description": self.__doc__ or "*No description provided.*",
            "definitions": self.definitions,
            "collections": {name: coll.schema_url for name, coll in self.collections.items()},
            "methods": {m.slug: m.schema for m in self.exposed_methods}
        }

    @property
    def full_schema(self):
        return {
            "id": self.url + ";schema",
            "type": "object",
            "description": self.__doc__ or "*No description provided.*",
            "definitions": self.definitions,
            "collections": {name: coll.schema for name, coll in self.collections.items()},
            "methods": {m.slug: m.schema for m in self.exposed_methods}
        }

    def help(self, out=None, initial_heading_level=0):
        """Return full reStructuredText help for this class"""
        builder = help.SchemaHelpBuilder(self.schema, self.url, out=out, initial_heading_level=initial_heading_level)
        builder.begin_subheading(self.name)
        builder.begin_list()
        builder.define("Suite", self.suite.base_url + ';help')
        builder.define("Schema URL", self.schema_url)
        builder.define("Anonymous Reads", "yes" if self.anonymous_reads else "no")
        builder.end_list()
        builder.build()
        builder.line()
        builder.begin_subheading("Collections")
        builder.begin_list()
        for name, coll in self.collections.items():
            builder.define(name, coll.url + ';help')
        builder.end_list()
        builder.end_subheading()
        return builder.rst


    def __init__(self, suite, name=None):
        self.suite = suite
        self.name = name or self.__class__.__name__
        self.slug = utils.camelcase_slugify(self.name)
        self.db = utils.convert_camelcase(self.name)
        self.connection = suite.connections[self.connection]
        self.collections = {}
        self._url = '/'.join((self.suite.base_url, self.slug))
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
