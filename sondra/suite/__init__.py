from collections.abc import Mapping
from abc import ABCMeta
from copy import copy
from functools import partial
from urllib.parse import urlparse
import rethinkdb as r
import logging
import logging.config

from sondra import help
from sondra.ref import Reference
from . import signals


DOCSTRING_PROCESSORS = {}
try:
    from docutils.core import publish_string
    from sphinxcontrib import napoleon

    def google_processor(s):
        return publish_string(str(napoleon.GoogleDocstring(s)), writer_name='html', settings_overrides={"stylesheet_path": "sondra/css/flasky.css"})

    def numpy_processor(s):
        return publish_string(str(napoleon.NumpyDocstring(s)), writer_name='html', settings_overrides={"stylesheet_path": "sondra/css/flasky.css"})

    DOCSTRING_PROCESSORS['google'] = google_processor
    DOCSTRING_PROCESSORS['numpy'] = numpy_processor
except ImportError:
    pass

try:
    from docutils.core import publish_string

    DOCSTRING_PROCESSORS['rst'] = partial(publish_string, writer_name='html', settings_overrides={"stylesheet_path": "sondra/css/flasky.css"})
except ImportError:
    pass

try:
    from markdown import markdown

    DOCSTRING_PROCESSORS['markdown'] = markdown
except ImportError:
    pass

DOCSTRING_PROCESSORS['preformatted'] = lambda x: "<pre>" + str(x) + "</pre>"


BASIC_TYPES = {
    "date": {
        "type": "object",
        "required": ["year"],
        "properties": {
            "year": {"type": "integer"},
            "month": {"type": "integer"},
            "day": {"type": "integer"}
        }
    },
    "datetime": {
        "type": "object",
        "allOf": [{"$ref": "#/definitions/date"}],
        "required": ["year","month","day","hour"],
        "properties": {
            "hour": {"type": "integer"},
            "minute": {"type": "integer"},
            "second": {"type": "number"},
            "timezone": {"type": "string", "default": "Z"}
        }
    },
    "timedelta": {
        "type": "object",
        "required": ["start", "end"],
        "properties": {
            "start": {"$ref": "#/definitions/datetime"},
            "end": {"$ref": "#/definitions/datetime"},
        },
        "definitions": {
            "datetime": {
                "type": "object",
                "allOf": [{"$ref": "#/definitions/date"}],
                "required": ["year","month","day","hour"],
                "properties": {
                    "hour": {"type": "integer"},
                    "minute": {"type": "integer"},
                    "second": {"type": "number"},
                    "timezone": {"type": "string", "default": "Z"}
                }
            }
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


class Suite(Mapping):
    """This is the "environment" for Sondra. Similar to a `settings.py` file in Django, it defines the
    environment in which all :class:`Application`s exist.

    The Suite is also a mapping type, and it should be used to access or enumerate all the :class:`Application` objects
    that are registered.

    Attributes:
        always_allowed_formats (set): A set of formats where a
        applications (dict): A mapping from application name to Application objects. Suite itself implements a mapping
            protocol and this is its backend.
        async (dict): (Unsupported)
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
        schema (dict): The schema of a suite is a dict where the keys are the names of :class:`Application` objects
            registered to the suite. The values are the schemas of the named app.  See :class:`Application` for more
            details on application schemas.
    """
    name = None
    applications = {}
    async = False
    base_url = "http://localhost:8000"
    logging = None
    docstring_processor_name = 'preformatted'
    allow_anonymous_formats = {'help', 'schema'}
    api_request_processors = ()
    definitions = BASIC_TYPES
    connection_config = {
        'default': {}
    }

    @property
    def url(self):
        return self.base_url_path

    @property
    def schema_url(self):
        return self.base_url_path + "/schema"

    @property
    def schema(self):
        return {
            "id": self.url + "/schema",
            "name": self.name,
            "type": None,
            "description": self.__doc__ or "*No description provided.*",
            "applications": {k: v.schema_url for k, v in self.applications.items()},
            "definitions": self.definitions
        }

    @property
    def full_schema(self):
        return {
            "id": self.url + ";schema",
            "name": self.name,
            "type": None,
            "description": self.__doc__ or "*No description provided.*",
            "applications": {k: v.full_schema for k, v in self.applications.items()},
            "definitions": self.definitions
        }

    def __init__(self):
        if self.logging:
            logging.config.dictConfig(self.logging)
        else:
            logging.basicConfig()

        self.log = logging.getLogger(self.__class__.__name__)  # use root logger for the environment

        signals.pre_init.send(self.__class__, isntance=self)

        self.connections = {name: r.connect(**kwargs) for name, kwargs in self.connection_config.items()}
        for name in self.connections:
            self.log.warning("Connection established to '{0}'".format(name))

        p_base_url = urlparse(self.base_url)
        self.base_url_scheme = p_base_url.scheme
        self.base_url_netloc = p_base_url.netloc
        self.base_url_path = p_base_url.path
        self.log.warning("Suite base url is: '{0}".format(self.base_url))

        self.docstring_processor = DOCSTRING_PROCESSORS[self.docstring_processor_name]
        self.log.info('Docstring processor is {0}')

        self.name = self.name or self.__class__.__name__
        self.description = self.__doc__ or "No description provided."

        signals.post_init.send(self.__class__, instance=self)

    def register_application(self, app):
        """This is called automatically whenever an Application object is constructed."""
        if app.slug in self.applications:
            self.log.error("Tried to register application '{0}' more than once.".format(app.slug))
            raise SuiteException("Tried to register multiple applications with the same name.")

        self.applications[app.slug] = app
        self.log.info('Registered application {0} to {1}'.format(app.__class__.__name__, app.url))

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
        if not url.startswith(self.base_url):
            return None
        else:
            return Reference(self, url).value

    def lookup_document(self, url):
        if not url.startswith(self.base_url):
            return None
        else:
            return Reference(self, url).get_document()

    @property
    def schema(self):
        ret = {
            "name": self.base_url,
            "description": self.description,
            "definitions": copy(BASIC_TYPES),
            "applications": {}
        }

        for app in self.applications.values():
            ret['applications'][app.name] = app.schema

        return ret