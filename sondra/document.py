from collections.abc import MutableMapping, Mapping
from abc import ABCMeta
from copy import deepcopy, copy
from functools import partial
from urllib.parse import urlparse
import json
import jsonschema
import rethinkdb as r
from datetime import date, datetime, timezone
import logging
import logging.config

from . import utils
from .ref import Reference

_validator = jsonschema.Draft4Validator

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
        "allOf": ["#/definitions/date"],
        "required": ["year","month","day","hour"],
        "properties": {
            "hour": {"type": "integer"},
            "minute": {"type": "integer"},
            "second": {"type": "float"},
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
                "allOf": ["#/definitions/date"],
                "required": ["year","month","day","hour"],
                "properties": {
                    "hour": {"type": "integer"},
                    "minute": {"type": "integer"},
                    "second": {"type": "float"},
                    "timezone": {"type": "string", "default": "Z"}
                }
            }
        }
    }
}


def to_ref(doc):
    if isinstance(doc, Document):
        return doc.url
    else:
        return doc

def from_ref(doc):
    env = Suite()
    if isinstance(doc, str):
        if doc.startswith(env.base_url):
            return env.lookup_document(doc)
        else:
            return doc
    else:
        return doc


references = partial(utils.mapjson, to_ref)
documents = partial(utils.mapjson, from_ref)

class ValidationError(Exception):
    pass

class ValueHandler(object):
    is_geometry = False

    def to_rql_repr(self, value):
        return value

    def from_rql_repr(self, value):
        return value


class Geometry(ValueHandler):
    is_geometry = True

    def __init__(self, *allowed_types):
        self.allowed_types = set(x.lower() for x in allowed_types) if allowed_types else None

    def to_rql_repr(self, value):
        if self.allowed_types:
            if value['type'].lower() not in self.allowed_types:
                raise ValidationError('value not in ' + ','.join(t for t in self.allowed_types))
        return r.geojson(value)

    def from_rql_repr(self, value):
        del value['$reql_type$']
        return value


class Date(ValueHandler):
    def to_rql_repr(self, value):
        if isinstance(value, dict):
            return r.date(value['year'], value.get('month', None), value.get('day', None))
        else:
            return r.date(value.year, value.month, value.day)

    def from_rql_repr(self, value):
        timestamp = r.epoch_time(value)
        return date.fromtimestamp(timestamp)


class Time(ValueHandler):
    def __init__(self, timezone='Z'):
        self.timezone = timezone

    def from_rql_tz(self, tz):
        if tz == 'Z':
            return 0
        else:
            posneg = -1 if tz[0] == '-' else 1
            hours, minutes = map(int, tz.split(":"))
            offset = posneg*(hours*60 + minutes)
            return offset

    def to_rql_repr(self, value):
        if isinstance(value, str):
            return r.iso8601(value).as_timezone(self.timezone)
        if isinstance(value, dict):
            return r.time(
                value.get('year', None),
                value.get('month', None),
                value.get('day', None),
                value.get('hour', None),
                value.get('minute', None),
                value.get('second', None),
                value.get('timezone', self.timezone),
            ).as_timezone(self.timezone)
        else:
            return r.iso8601(value.isoformat()).as_timezone(self.timezone)

    def from_rql_repr(self, value):
        timestamp = value.to_epoch_time()
        tz = value.timezone()
        offset = self.from_rql_tz(tz)
        offset_tz = timezone(offset)
        dt = datetime.fromtimestamp(timestamp, tz=offset_tz)
        return dt


class EnvironmentException(Exception):
    pass


class ApplicationException(Exception):
    pass


class CollectionException(Exception):
    pass


class Singleton(type):
    instance = None
    def __call__(cls, *args, **kw):
        if not cls.instance:
             cls.instance = super(Singleton, cls).__call__(*args, **kw)
        return cls.instance


class SuiteMetaclass(ABCMeta):
    instance = None
    def __init__(cls, name, bases, nmspc):
        super(SuiteMetaclass, cls).__init__(name, bases, nmspc)
        cls.name = utils.convert_camelcase(name)
        if not hasattr(cls, 'registry'):
            cls.registry = set()
        cls.registry.add(cls)
        cls.registry -= set(bases) # Remove base classes

    def __call__(cls, *args, **kwargs):
        if len(cls.registry) > 1:
            raise EnvironmentException("There can only be one final environment class")

        if not cls.instance:
            c = next(iter(cls.registry))
            cls.instance = super(SuiteMetaclass, c).__call__(*args, **kwargs)
        return cls.instance


class Suite(Mapping, metaclass=SuiteMetaclass):
    applications = {}
    async = False
    base_url = "http://localhost:8000"
    logging = None
    connections = {
        'default': {}
    }

    def __init__(self):
        self.connections = {name: r.connect(**kwargs) for name, kwargs in self.connections.items()}
        self._prefix_len = len(self.base_url)
        p_base_url = urlparse(self.base_url)

        #: str: http or https
        self.base_url_scheme = p_base_url.scheme

        #: str: host
        self.base_url_netloc = p_base_url.netloc

        #: str: offset path for all applications.  Not supported currently.
        self.base_url_path = p_base_url.path

        if self.logging:
            logging.config.dictConfig(self.logging)

        self.log = logging  # use root logger for the environment

        try:
            from docutils.core import publish_string
            self.docstring_processor = partial(publish_string, writer_name='html')

        except ImportError:
            try:
                from markdown import markdown
                self.docstring_processor = markdown
            except ImportError:
                self.docstring_processor = lambda x: "<pre>" + str(x) + "</pre>"

    def register_application(self, app):
        if app.slug in self.applications:
            self.log.error("Tried to register application '{0}' more than once.".format(app.slug))
            raise EnvironmentException("Tried to register multiple applications with the same name.")

        self.applications[app.slug] = app
        self.log.info('Registered application {0} to {1}'.format(app.__class__.__name__, app.url))


    def __getitem__(self, item):
        return self.applications[item]

    def __len__(self):
        return len(self.applications)

    def __iter__(self):
        return iter(self.applications)

    def __contains__(self, item):
        return item in self.applications

    def lookup(self, url):
        if not url.startswith(self.base_url):
            return None
        else:
            return Reference(Suite(), url).value

    def lookup_document(self, url):
        if not url.startswith(self.base_url):
            return None
        else:
            return Reference(Suite(), url).get_document()

    @property
    def schema(self):
        ret = {
            "name": self.base_url,
            "description": self.__doc__,
            "definitions": copy(BASIC_TYPES)
        }

        for app in self.applications.values():
            ret['definitions'][app.name] = app.schema

        return ret



class CollectionMetaclass(ABCMeta):
    def __init__(cls, name, bases, nmspc):
        super(CollectionMetaclass, cls).__init__(name, bases, nmspc)
        cls.name = utils.convert_camelcase(cls.__name__)
        cls.slug = utils.camelcase_slugify(cls.__name__)

        cls.schema = deepcopy(cls.document_class.schema)
        if 'description' not in cls.schema:
            cls.schema['description'] = cls.__doc__ or "No description provided"
        if 'id' in cls.schema['properties']:
            raise CollectionException('Document schema should not have an "id" property')
        if not cls.primary_key:
            cls.schema['properties']['id'] = {"type": "string"}

        _validator.check_schema(cls.schema)

        if not hasattr(cls, 'application') or cls.application is None:
            raise CollectionException("{0} declared without application".format(name))
        else:
            cls.application.register_collection(cls)


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

    def __call__(cls, *args, **kwargs):
        instance = super(ApplicationMetaclass, cls).__call__(*args, **kwargs)
        Suite().register_application(instance)
        return instance


class Application(Mapping, metaclass=ApplicationMetaclass):
    db = 'default'
    connection = 'default'
    slug = None
    collections = None

    def __init__(self, name=None):
        self.env = Suite()
        self.name = name or self.__class__.__name__
        self.slug = utils.camelcase_slugify(self.name)
        self.db = utils.convert_camelcase(self.name)
        self.connection = Suite().connections[self.connection]
        self.collections = {}
        self.url = '/'.join((self.env.base_url, self.slug))
        self.log = logging.getLogger(self.name)
        self.application = self

        self.before_init()
        for name, collection_class in self.__class__:
            self.collections[name] = collection_class(self)
        self.after_init()

    def __len__(self):
        return len(self.collections)

    def __getitem__(self, item):
        return self.collections[item]

    def __iter__(self):
        return iter(self.collections)

    def __contains__(self, item):
        return item in self.collections

    def create_tables(self, *args, **kwargs):
        for collection_class in self.collections.values():
            collection_class.create_table(*args, **kwargs)

    def drop_tables(self, *args, **kwargs):
        for collection_class in self.collections.values():
            collection_class.drop_table(*args, **kwargs)

    def create_database(self):
        try:
            r.db_create(self.db).run(self.connection)
        except r.ReqlError as e:
            self.log.info(e.message)

    def drop_database(self):
        r.db_drop(self.db).run(self.connection)

    def after_init(self):
        pass

    def before_init(self):
        pass

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


class Document(MutableMapping):
    schema = {
        "type": "object",
        "properties": {}
    }

    def __init__(self, obj, collection=None, parent=None):
        self.collection = collection
        if not self.collection and (parent and parent.collection):
            self.collection = parent.collection

        self.parent = parent

        self.url = None
        if self.collection.primary_key in obj:
            self.url = '/'.join((self.collection.url, obj[self.collection.primary_key]))

        self._referenced = True
        self.obj = {}
        for k, v in obj.items():
            self[k] = v

    @property
    def id(self):
        return self.obj.get(self.collection.primary_key, None)

    @property
    def application(self):
        return self.collection.application

    @id.setter
    def id(self, v):
        self.obj[self.collection.primary_key] = v

    @property
    def name(self):
        return self.id

    @property
    def slug(self):
        return self.id

    def __len__(self):
        return len(self.obj)

    def __eq__(self, other):
        return self.id and (self.id == other.id)

    def __getitem__(self, key):
        return self.obj[key]

    def __setitem__(self, key, value):
        if isinstance(value, Document):
            value.parent = self
            self.referenced = False
        self.obj[key] = value

    def __delitem__(self, key):
        del self.obj[key]

    def __iter__(self):
        return iter(self.obj)

    def json(self, *args, **kwargs):
        if not self._referenced:
            self.reference()
        return json.dumps(self.obj, *args, **kwargs)

    def save(self, *args, **kwargs):
        return self.collection.save(self.obj, *args, **kwargs)

    def delete(self, **kwargs):
        return  self.collection.delete(self.id, **kwargs)

    def dereference(self):
        self.obj = documents(self.obj)
        self._referenced = False
        return self

    def reference(self):
        self.obj = references(self.obj)
        self._referenced = True
        return self

    def validate(self):
        jsonschema.validate(self.obj, self.schema)


class Collection(MutableMapping, metaclass=CollectionMetaclass):
    name = None
    slug = None
    schema = None
    application = Application
    document_class = Document
    primary_key = "id"
    specials = {}
    indexes = []
    relations = []

    @property
    def table(self):
        return r.db(self.application.db).table(self.name)

    def __init__(self, application):
        self.application = application
        self.url = '/'.join((self.application.url, self.slug))
        self.schema['id'] = self.url + ";schema"
        self.log = logging.getLogger(self.application.name + "." + self.name)
        self.after_init()

    def create_table(self, *args, **kwargs):
        self.before_table_create()

        try:
            r.db(self.application.db)\
                .table_create(self.name, primary_key=self.primary_key, *args, **kwargs)\
                .run(self.application.connection)
        except r.ReqlError as e:
            self.log.info('Table {0}.{1} already exists.'.format(self.application.db, self.name))

        for index in self.indexes:
            if isinstance(index, tuple):
                index, index_function = index
            else:
                index_function = None

            if self.schema['properties'][index].get('type', None) == 'array':
                multi = True
            else:
                multi = False

            if index in self.specials and self.specials[index].is_geometry:
                geo = True
            else:
                geo = False

            try:
                if index_function:
                    self.table.index_create(index, index_function, multi=multi, geo=geo).run(self.application.connection)
                else:
                    self.table.index_create(index, multi=multi, geo=geo).run(self.application.connection)
            except r.ReqlError as e:
                self.log.info('Index on table {0}.{1} already exists.'.format(self.application.db, self.name))

        self.after_table_create()

    def drop_table(self):
        self.before_table_drop()
        ret = r.db(self.application.db).table_drop(self.name).run(self.application.connection)
        self.log.info('Dropped table {0}.{1}'.format(self.application.db, self.name))
        self.after_table_drop()
        return ret

    def _from_rql_repr(self, doc):
        for property, special in self.specials.items():
            if property in doc:
                doc[property] = special.from_rql_repr(doc[property])

    def _to_rql_repr(self, doc):
        for property, special in self.specials.items():
            if property in doc:
                doc[property] = special.to_rql_repr(doc[property])

    def __getitem__(self, key):
        doc = self.table.get(key).run(self.application.connection)
        if doc:
            self._from_rql_repr(doc)
            return self.document_class(doc, collection=self)
        else:
            raise KeyError('{0} not found in {1}'.format(key, self.url))

    def __setitem__(self, key, value):
        return self.save(value, conflict='replace').run(self.application.connection)

    def __delitem__(self, key):
        self.before_delete()
        self.table.get(key).delete().run(self.application.connection)
        self.after_delete()

    def __iter__(self):
        for doc in self.table.run(self.application.connection):
            self._from_rql_repr(doc)
            yield doc

    def __contains__(self, item):
        doc = self.table.get(item).run(self.application.connection)
        return doc is not None

    def __len__(self):
        return self.table.count().run(self.application.connection)

    def q(self, query):
        for doc in query.run(self.application.connection):
            self._from_rql_repr(doc)
            yield self.document_class(doc, collection=self)

    def doc(self, kwargs):
        return self.document_class(kwargs, collection=self)

    def create(self,kwargs):
        doc = self.document_class(kwargs, collection=self)
        self.save(doc, conflict="error")
        return doc

    def validator(self, value):
        return True

    def before_validation(self):
        pass

    def before_save(self):
        pass

    def after_save(self):
        pass

    def before_delete(self):
        pass

    def after_delete(self):
        pass

    def before_table_create(self):
        pass

    def after_table_create(self):
        pass

    def before_table_drop(self):
        pass

    def after_table_drop(self):
        pass

    def after_init(self):
        pass

    def delete(self, docs, **kwargs):
        if not isinstance(docs, list):
            docs = [docs]

        values = [v.id if isinstance(v, Document) else v for v in docs]
        return self.table.get_all(*values).delete(**kwargs).run(self.application.connection)

    def save(self, docs, **kwargs):
        if not isinstance(docs, list):
            docs = [docs]

        values = []
        for value in docs:
            if isinstance(value, Document):
                value = copy(value.obj)

            self.before_validation()
            value = references(value)  # get rid of Document objects and turn them into URLs
            jsonschema.validate(value, self.schema)
            self.validator(value)

            self.before_save()
            self._to_rql_repr(value)
            values.append(value)
            self.after_save()

        return self.table.insert(*values, **kwargs).run(self.application.connection)



