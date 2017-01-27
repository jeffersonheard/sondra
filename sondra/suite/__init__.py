from collections.abc import Mapping
from abc import ABCMeta
from functools import partial
import importlib
from urllib.parse import urlparse
import requests
import rethinkdb as r
import logging
import logging.config
import os

from jsonschema import Draft4Validator

from sondra import help
from sondra.api.ref import Reference
from sondra.schema import merge
from . import signals

CSS_PATH = os.path.join(os.getcwd(), 'static', 'css', 'help.css')
DOCSTRING_PROCESSORS = {}
try:
    from docutils.core import publish_string
    from sphinxcontrib import napoleon


    def google_processor(s):
        return publish_string(
            str(napoleon.GoogleDocstring(s)),
            writer_name='html',
            settings_overrides={
                "stylesheet_path": CSS_PATH,
                "embed_stylesheet": True,
                'report_level': 5
            }
        )

    def numpy_processor(s):
        return publish_string(
            str(napoleon.NumpyDocstring(s)),
            writer_name='html',
            settings_overrides={
                "stylesheet_path": CSS_PATH,
                "embed_stylesheet": True,
                'report_level': 5
            }
        )

    DOCSTRING_PROCESSORS['google'] = google_processor
    DOCSTRING_PROCESSORS['numpy'] = numpy_processor
except ImportError:
    pass

try:
    from docutils.core import publish_string

    DOCSTRING_PROCESSORS['rst'] = partial(publish_string, writer_name='html', settings_overrides={"stylesheet_path": CSS_PATH, "embed_stylesheet": True})
except ImportError:
    pass

try:
    from markdown import markdown

    DOCSTRING_PROCESSORS['markdown'] = markdown
except ImportError:
    pass

DOCSTRING_PROCESSORS['preformatted'] = lambda x: "<pre>" + str(x) + "</pre>"


BASIC_TYPES = {
    "timedelta": {
        "type": "object",
        "required": ["start", "end"],
        "properties": {
            "days": {"type": "integer"},
            "hours": {"type": "integer"},
            "minutes": {"type": "integer"},
            "seconds": {"type": "number"}
        }
    },
    "filterOps": {
        "enum": [
            'with_fields',
            'count',
            'max',
            'min',
            'avg',
            'sample',
            'sum',
            'distinct',
            'contains',
            'pluck',
            'without',
            'has_fields',
            'order_by',
            'between'
        ]
    },
    "spatialOps": {
        "enum": [
            'distance',
            'get_intersecting',
            'get_nearest',
        ]
    }
}


class SuiteException(Exception):
    """Represents a misconfiguration in a :class:`Suite` class"""


class SuiteMetaclass(ABCMeta):
    def __new__(mcs, name, bases, attrs):
        definitions = {}
        for base in bases:
            if hasattr(base, "definitions") and base.definitions:
                definitions.update(base.definitions)

        if "definitions" in attrs:
            attrs['definitions'].update(definitions)
        else:
            attrs['definitions'] = definitions

        return super().__new__(mcs, name, bases, attrs)

    def __init__(self, name, bases, attrs):
        super(SuiteMetaclass, self).__init__(name, bases, attrs)
        url = "http://localhost:5000/api"
        for base in bases:
            if hasattr(base, 'url'):
                url = base.url

        attrs['url'] = attrs.get('url', url)
        p_base_url = urlparse(attrs['url'])
        self.base_url_scheme = p_base_url.scheme
        self.base_url_netloc = p_base_url.netloc
        self.base_url_path = p_base_url.path
        self.slug = self.base_url_path[1:] if self.base_url_path  else ""


class Suite(Mapping, metaclass=SuiteMetaclass):
    """This is the "environment" for Sondra. Similar to a `settings.py` file in Django, it defines the
    environment in which all :class:`Application`s exist.

    The Suite is also a mapping type, and it should be used to access or enumerate all the :class:`Application` objects
    that are registered.

    Attributes:
        always_allowed_formats (set): A set of formats where a
        applications (dict): A mapping from application name to Application objects. Suite itself implements a mapping
            protocol and this is its backend.
        base_url (str): The base URL for the API. The Suite will be mounted off of here.
        base_url_scheme (str): http or https, automatically set.
        base_url_netloc (str): automatically set hostname of the suite.
        connection_config (dict): For each key in connections setup keyword args to be passed to `rethinkdb.connect()`
        connections (dict): RethinkDB connections for each key in ``connection_config``
        docstring_processor_name (str): Any member of DOCSTRING_PROCESSORS: ``preformatted``, ``rst``, ``markdown``,
            ``google``, or ``numpy``.
        docstring_processor (callable): A ``lambda (str)`` that returns HTML for a docstring.
        logging (dict): A dict-config for logging.
        log (logging.Logger): A logger object configured with the above dictconfig.
        cross_origin (bool=False): Allow cross origin API requests from the browser.
        db_prefix: (str=""): A default string to prepend to all the database names in this suite.
        schema (dict): The schema of a suite is a dict where the keys are the names of :class:`Application` objects
            registered to the suite. The values are the schemas of the named app.  See :class:`Application` for more
            details on application schemas.
    """
    title = "Sondra-Based API"
    name = None
    debug = False
    applications = None
    definitions = BASIC_TYPES
    url = "http://localhost:5000/api"
    logging = None
    log_level = None
    docstring_processor_name = 'preformatted'
    cross_origin = False
    allow_anonymous_formats = {'help', 'schema'}
    api_request_processors = ()
    file_storage = None
    connection_config = {
        'default': {}
    }
    working_directory = os.getcwd()
    language = 'en'
    translations = None
    db_prefix = ""

    @property
    def schema_url(self):
        return self.url + ";schema"

    @property
    def schema(self):
        return {
            "id": self.url + ";schema",
            "title": self.title,
            "type": "object",
            "description": self.__doc__ or "*No description provided.*",
            "applications": {k: v.url for k, v in self.applications.items()},
            "definitions": self.definitions
        }

    @property
    def full_schema(self):
        return {
            "id": self.url + ";schema",
            "title": self.title,
            "type": None,
            "description": self.__doc__ or "*No description provided.*",
            "applications": {k: v.full_schema for k, v in self.applications.items()},
            "definitions": self.definitions
        }

    def __init__(self, db_prefix=""):
        self.applications = {}
        self.connections = None
        self.db_prefix = db_prefix

        if self.logging:
            logging.config.dictConfig(self.logging)
        elif self.log_level:
            logging.basicConfig(level=self.log_level)
        else:
            logging.basicConfig(level=logging.DEBUG if self.debug else logging.INFO)

        self.log = logging.getLogger(self.__class__.__name__)  # use root logger for the environment

        signals.pre_init.send(self.__class__, isntance=self)

        self.connect()
        for name in self.connections:
            self.log.info("Connection established to '{0}'".format(name))

        self.log.info("Suite base url is: '{0}".format(self.url))

        self.docstring_processor = DOCSTRING_PROCESSORS[self.docstring_processor_name]
        self.log.info('Docstring processor is {0}')

        self.log.info('Default language is {0}'.format(self.language))
        self.log.info('Translations for default language are {0}'.format('present' if self.translations else 'not present'))

        self.name = self.name or self.__class__.__name__
        self.description = self.__doc__ or "No description provided."
        signals.post_init.send(self.__class__, instance=self)

    def check_connections(self):
        for name, conn in self.connections.items():
            try:
                r.db_list().run(conn)
            except r.ReqlDriverError as e:
                self.connections[name] = r.connect(**self.connection_config[name])

    def connect(self):
        self.connections = {name: r.connect(**kwargs) for name, kwargs in self.connection_config.items()}

    def register_application(self, app):
        """This is called automatically whenever an Application object is constructed."""
        if app.slug in self.applications:
            self.log.error("Tried to register application '{0}' more than once.".format(app.slug))
            raise SuiteException("Tried to register multiple applications with the same name.")

        self.applications[app.slug] = app
        self.log.info('Registered application {0} to {1}'.format(app.__class__.__name__, app.url))

    def set_language_for_schema(self, app, collection, schema, definitions):
        """Takes translations and applies them to the schema, overriding enumNames, title, and description where appropriate."""
        self.log.info('Applying localization "{0}" for {1}/{2}'.format(self.language, app, collection))

        # update schema
        if (
           self.language and
           self.translations and
           self.language in self.translations and
           app in self.translations[self.language] and
           collection in self.translations[self.language][app]
        ):
            translated_schema = self.translations[self.language][app][collection]
            for key in translated_schema:
                schema_to_update = schema
                full_key = key.split('.')
                while full_key:
                    schema_to_update = schema_to_update['properties'][full_key.pop(0)]
                schema_to_update.update(translated_schema[key])

        # update definitions
        if (
            self.language and
            self.translations and
            self.language in self.translations and
            'definitions' in self.translations[self.language]
        ):
            translated_definitions = self.translations[self.language]['definitions']
            for key in translated_definitions:
                entry, *full_key = key.split('.')
                if entry in definitions:
                    schema_to_update = definitions[entry]
                    while full_key:
                        schema_to_update = schema_to_update['properties'][full_key.pop(0)]
                    schema_to_update.update(translated_definitions[key])

                if entry in schema.get('definitions', {}):
                    schema_to_update = schema['definitions'][entry]
                    while full_key:
                        schema_to_update = schema_to_update['properties'][full_key.pop(0)]
                    schema_to_update.update(translated_definitions[key])

    def set_language(self):
        for app_key, collections in self.items():
            for coll_key, coll in collections.items():
                self.set_language_for_schema(app_key, coll_key, coll.schema, coll.definitions)

    def drop_database_objects(self):
        for app in self.values():
            app.drop_database()

    def ensure_database_objects(self):
        for app in self.values():
            app.create_database()
            app.create_tables()

    def clear_databases(self):
        self.drop_database_objects()
        self.ensure_database_objects()

    def clear_tables(self):
        for app in self.values():
            app.clear_tables()

    def __getitem__(self, item):
        """Application objects are indexed by "slug." Every Application object registered has its name slugified.

        This means that if your app was called `MyCoolApp`, its registered name would be `my-cool-app`. This key is
        used whether you are accessing the application via URL or locally via Python.  For example, the following
        both produce the same result::

            URL (yields schema as application/json):

                http://localhost:5000/api/my-cool-app;schema

            Python (yields schema as a dict):

                suite = Suite()
                suite['my-cool-app'].schema
        """
        return self.applications[item]

    def __len__(self):
        return len(self.applications)

    def __iter__(self):
        return iter(self.applications)

    def __contains__(self, item):
        return item in self.applications

    def help(self, out=None, initial_heading_level=0):
        """Return full reStructuredText help for this class"""
        builder = help.SchemaHelpBuilder(self.schema, self.url, out=out, initial_heading_level=initial_heading_level)
        builder.begin_subheading(self.name)
        builder.define("Suite", self.url)
        builder.line()
        builder.define("Schema URL", self.schema_url)
        builder.line()
        builder.build()
        builder.line()
        builder.begin_subheading("Applications")
        builder.begin_list()
        for name, coll in self.applications.items():
            builder.define(name, coll.url + ';help')
        builder.end_list()
        builder.end_subheading()
        builder.end_subheading()
        return builder.rst

    def lookup(self, url):
        if not url.startswith(self.url):
            return requests.get(url).json()  # TODO replace with client.
        else:
            return Reference(self, url).value

    def lookup_document(self, url):
        if not url.startswith(self.url):
            return requests.get(url).json()  # TODO replace with client.
        else:
            return Reference(self, url).get_document()

    def validate(self):
        self.log.info("Checking schemas for validity")
        for application in self.applications.values():
            self.log.info("+ " + application.slug)
            for collection in application.collections:
                self.log.info('--- ' + collection.slug)
                Draft4Validator.check_schema(collection.schema)

