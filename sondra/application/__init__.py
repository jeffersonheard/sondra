from blinker import signal
from collections import OrderedDict
from collections.abc import Mapping
from abc import ABCMeta

import rethinkdb as r
import logging
import logging.config


from sondra import help, utils
from . import signals
from sondra.utils import mapjson


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
        for method in (n for n in attrs.values() if hasattr(n, 'exposed')):
                cls.exposed_methods[method.slug] = method


class Application(Mapping, metaclass=ApplicationMetaclass):
    """An Application groups collections that serve a related purpose.

    In addition to collections, methods and schemas can be exposed at the application level. Any exposed methods
    might be termed "library functions" in the sense that they apply to all collections, or configure the collections
    at a high level. Schemas exposed on the application level should be common to several collections or somehow
    logically "broader" than definitions at the document/collection level.

    Application behave as Python dicts. The keys in the application's dictionary are the slugs of the collections the
    application houses. In addition, applications are stored as items in a Suite's dictionary. Thus to access the
    'Users' collection in the 'Auth' application, one could start with the suite and work down thus::

        > suite['auth']['users']
        ...
        <Application object 0x...>

    Also see the `webservices reference`_ for more on how to access applications and their schemas and
    methods over the web.

    Attributes:
        db (str): The name of a RethinkDB database
        connection (str): The name of a RethinkDB connection in the application's suite
        slug (str): read-only. The name of this application class, slugified (all lowercase, and separate words with -)
        anonymous_reads (bool=True): Override this attribute in your subclass if you want to disable anonymous queries
          for help and schema for this application and all its collections.
        definitions (dict): This should be a JSON serializable dictionary of schemas. Each key will be the name of that
          schema definiton in this application schema's "definitions" object.
        url (str): read-only. The full URL for this application.
        schema_url (str): read-only. A shortcut to the application's schema URL.
        schema (dict): read-only. A JSON serializable schema definition for this application. In addition to the
          standard JSON-schema definitions, a dictionary of collection schema URLs and a list of methods are included as
          "collections" and "methods" respectively.  The keys for the collection schemas are the slugged names of the
          collections themselves.
        full_schema (dict): Same as schema, except that collections are fully defined instead of merely referenced in
          the "collections" sub-object.

    ..webservices reference: /docs/web-services.html
    """
    db = 'default'
    connection = 'default'
    title = None
    slug = None
    collections = ()
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
        ret = {
            "id": self.url + ";schema",
            "title": self.title or self.__class__.__name__,
            "type": "object",
            "description": self.__doc__ or "*No description provided.*",
            "definitions": self.definitions,
            "collections": {name: coll.schema_url for name, coll in self._collections.items()},
            "methods": [m.slug for m in self.exposed_methods.values()]
        }
        ret = mapjson(lambda x: x(context=self.suite) if callable(x) else x, ret)
        return ret

    @property
    def full_schema(self):
        return {
            "id": self.url + ";schema",
            "type": "object",
            "description": self.__doc__ or "*No description provided.*",
            "definitions": self.definitions,
            "collections": {name: coll.schema for name, coll in self._collections.items()},
            "methods": [m.slug for m in self.exposed_methods.values()]
        }

    def help(self, out=None, initial_heading_level=0):
        """Return full reStructuredText help for this class.

        Args:
            out (io): An output, usually io.StringIO
            initial_heading_level (int): 0-5, default 0. The heading level to start at, in case this is being included
              as part of a broader help scheme.

        Returns:
            (str): reStructuredText help.
        """
        builder = help.SchemaHelpBuilder(self.schema, self.url, out=out, initial_heading_level=initial_heading_level)
        builder.begin_subheading(self.name)
        builder.begin_list()
        builder.define("Suite", self.suite.base_url + '/help')
        builder.define("Schema URL", self.schema_url)
        builder.define("Anonymous Reads", "yes" if self.anonymous_reads else "no")
        builder.end_list()
        builder.build()
        builder.line()

        if self.exposed_methods:
            builder.begin_subheading("Methods")
            for name, method in sorted(self.exposed_methods.items(), key=lambda x: x[0]):
                new_builder = help.SchemaHelpBuilder(method.schema(getattr(self, method.__name__)), initial_heading_level=builder._heading_level)
                new_builder.build()
                builder.line(new_builder.rst)
            builder.end_subheading()

        builder.begin_subheading("Collections")
        builder.begin_list()
        for name, coll in sorted(self._collections.items(), key=lambda x: x[0]):
            builder.define(name, coll.url + ';help')
        builder.end_list()
        builder.end_subheading()

        return builder.rst


    def __init__(self, suite, name=None):
        """Create a new instance of the application.

        Args:
            suite (sondra.suite.Suite): The suite with which to register the application.
            name (str): If supplied, this is the name of the app.  Otherwise, the name is the same as the classname
              The application slug is the slugified version of the name.
        """
        self.suite = suite
        self.name = name or self.__class__.__name__
        self.slug = utils.camelcase_slugify(self.name)
        self.db = utils.convert_camelcase(self.name)
        self.connection = suite.connections[self.connection]
        self._collections = {}
        self._url = '/'.join((self.suite.base_url, self.slug))
        self.log = logging.getLogger(self.name)
        self.application = self

        signals.pre_registration.send(self.__class__, instance=self)
        self.log.warning("Registering application {0}".format(self.slug))
        suite.register_application(self)
        signals.post_registration.send(self.__class__, instance=self)

        signals.pre_init.send(self.__class__, instance=self)
        for collection_class in self.collections:
            name = collection_class.slug
            self.log.warning("Creating collection for {0}/{1}".format(self.slug, collection_class.slug))
            self._collections[name] = collection_class(self)
        signals.post_init.send(self.__class__, instance=self)

    def __len__(self):
        return len(self._collections)

    def __getitem__(self, item):
        return self._collections[item]

    def __iter__(self):
        return iter(self._collections)

    def __contains__(self, item):
        return item in self._collections

    def create_tables(self, *args, **kwargs):
        """Create tables in the db for all collections in the application.

        If the table exists, log a warning.

        **Signals sent:**

        pre_create_tables(instance=``self``, args=``args``, kwargs=``kwargs``)
          Sent before tables are created

        post_create_tables(instance=``self``)
          Sent after the tables are created

        Args:
            *args: Sent to collection.create_table as vargs.
            **kwargs: Sent to collection.create_table as keyword args.

        Returns:
            None
        """
        signals.pre_create_tables.send(self.__class__, instance=self, args=args, kwargs=kwargs)

        for collection_class in self._collections.values():
            try:
                collection_class.create_table(*args, **kwargs)
            except Exception as e:
                self.log.warning(str(e))

        signals.post_create_tables.send(self.__class__, instance=self)

    def drop_tables(self, *args, **kwargs):
        """Create tables in the db for all collections in the application.

        If the table exists, log a warning.

        **Signals sent:**

        pre_delete_tables(instance=``self``, args=``args``, kwargs=``kwargs``)
          Sent before tables are created

        post_delete_tables(instance=``self``)
          Sent after the tables are created

        Args:
            *args: Sent to collection.delete_table as vargs.
            **kwargs: Sent to collection.delete_table as keyword args.

        Returns:
            None
        """

        signals.pre_delete_tables.send(self.__class__, instance=self, args=args, kwargs=kwargs)

        for collection_class in self._collections.values():
            try:
                collection_class.drop_table(*args, **kwargs)
            except Exception as e:
                self.log.warning(str(e))

        signals.post_delete_tables.send(self.__class__, instance=self)

    def create_database(self):
        """Create the db for the application.

        If the db exists, log a warning.

        Returns:
            None
        """

        try:
            r.db_create(self.db).run(self.connection)
        except r.ReqlError as e:
            self.log.warning(str(e))

    def drop_database(self):
        """Drop the db for the application.

        If the db exists, log a warning.

        Returns:
            None
        """

        try:
            r.db_create(self.db).run(self.connection)
        except r.ReqlError as e:
            self.log.warning(str(e))
