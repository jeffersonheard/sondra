from datetime import datetime, date, timezone
from collections.abc import MutableMapping
from abc import ABCMeta
from copy import deepcopy, copy
import iso8601
import jsonschema
import rethinkdb as r
import logging
import logging.config
from slugify import slugify
from shapely.geometry import mapping, shape
from shapely.geometry.base import BaseGeometry

from sondra import help, utils
from sondra.application import Application
from sondra.document import Document, references, signals as doc_signals, ValidationError

from . import signals

_validator = jsonschema.Draft4Validator

class CollectionException(Exception):
    """Represents a misconfiguration in a :class:`Collection` class definition"""


class CollectionMetaclass(ABCMeta):
    """The metaclass sets name and schema and registers this collection with an application.

    The schema description is updated with the docstring of the concrete collection class. The title is set to the name
    of the class, if it is not already set.

    This metaclass also post-processes inheritance, so that:

    * definitions from base classes are included in subclasses.
    * exposed methods in base classes are included in subclasses.
    """

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

    def __init__(cls, name, bases, nmspc):
        super(CollectionMetaclass, cls).__init__(name, bases, nmspc)
        cls.exposed_methods = {}
        for base in bases:
            if hasattr(base, 'exposed_methods'):
                cls.exposed_methods.update(base.exposed_methods)
        for name, method in (n for n in nmspc.items() if hasattr(n[1], 'exposed')):
                cls.exposed_methods[name] = method

        cls.name = utils.convert_camelcase(cls.__name__)

        cls.schema = deepcopy(cls.document_class.schema)

        if (not cls.__doc__) and cls.document_class.__doc__:
            cls.__doc__ = cls.document_class.__doc__

        if 'description' not in cls.schema and cls.__doc__:
            cls.schema['description'] = cls.__doc__

        if "title" not in cls.schema:
            cls.schema['title'] = cls.__name__

        if "definitions" in cls.schema:
            cls.schema['definitions'].update(cls.definitions)
        else:
            cls.schema['definitions'] = cls.definitions

        if 'id' in cls.schema['properties']:
            raise CollectionException('Document schema should not have an "id" property')

        if not cls.primary_key:
            cls.schema['properties']['id'] = {"type": "string"}

        cls.schema["methods"] = [m.slug for m in cls.exposed_methods.values()]
        cls.schema["documentMethods"] = [m.slug for m in cls.document_class.exposed_methods.values()]

        _validator.check_schema(cls.schema)

        if not hasattr(cls, 'application') or cls.application is None:
            cls.abstract = True
        else:
            cls.abstract = False

        if not cls.abstract:
            cls.slug = utils.camelcase_slugify(cls.__name__)
            cls.application.register_collection(cls)


class Collection(MutableMapping, metaclass=CollectionMetaclass):
    """The collection is the workhorse of Sondra.

    Collections are mutable mapping types, like dicts, whose keys are the keys in the database collection. The database
    table, or collection, has the same name as the collection's slug with hyphens replaced by underscores for
    compatibility.

    Collections expand on a document schema, specifying:

    * properties that should be treated specially.
    * the primary key
    * indexes that should be built
    * relationships to other collections

    Like applications, collections have webservice endpoints::

        http://localhost:5000/application/collection;(schema|help|json|geojson)
        http://localhost:5000/application/collection.method;(schema|help|json)

    These endpoints allow the user to create, update, filter, list, and delete objects in the collection, which are
    individual documents. Also, any methods that are exposed by the ``sondra.decorators.expose`` decorator are exposed
    as method endpoints.

    To use Python to retrieve individual Document instances, starting with the suite::

        > suite['app-name']['collection-name']['primary-key']
        ...
        <sondra.document.Document object at 0x....>

    Special properties
    ------------------
    The ``specials`` attribute is a dictionary of property names to ``sondra.document.ValueHandler`` instances, which
    tell the collection how to handle properties containing objects that aren't standard JSON. This includes date-time
    objects and geometry, which is handled via the `Shapely`_ library. Shapely is not supported by readthedocs, so you
    must install it separately.  See the individual ValueHandler subclasses in sondra.document for more information.

    Attributes:
        name (str): read-only. The name of the collection, based on the classname.
        slug (str): read-only. The hyphen separated name of the collection, based on the classname
        schema (str): read-only. The collection's schema, based on the ``document_class``
        suite (sondra.suite.Suite): read-only. The suite this collection's application is a part of. None for abstract
          classes.
        application (sondra.application.Application). The application this collection is a part of. None for abstract
          classes.
        document_class (sondra.document.Document): The document class this collection contains. The schema is derived
          from this.
        primary_key (str): The field (if different from id) to use as the primary key. Individual documents are
          referenced by primary key, both in the Python interface and the webservice interface.
        private (bool=False). If the collection is private, then it is not exposed by the webservice interface. This
          can be very useful for collections whose data should never be available over the 'net.
        specials (dict): A dictionary of properties to be treated specially.
        indexes ([str])
        relations (dict)
        anonymous_reads (bool=True)
        abstract (bool)
        table (ReQL)
        url (str)
        schema_url (str)

    .. _Shapely: https://pypi.python.org/pypi/Shapely

    """

    name = None
    slug = None
    schema = None
    application = Application
    document_class = Document
    primary_key = "id"
    private = False
    specials = {}
    indexes = []
    relations = []
    anonymous_reads = True
    abstract = False
    processors = []

    @property
    def suite(self):
        if self.application:
            return self.application.suite
        else:
            return None

    @property
    def table(self):
        return r.db(self.application.db).table(self.name)

    @property
    def url(self):
        if self._url:
            return self._url
        elif self.application:
            return self.application.url + "/" + self.slug
        else:
            return self.slug

    @property
    def schema_url(self):
        return self.url + ";schema"''

    def __init__(self, application):
        signals.pre_init.send(self.__class__, instance=self)
        self.application = application
        self._url = '/'.join((self.application.url, self.slug))
        self.schema['id'] = self.url + ";schema"
        self.log = logging.getLogger(self.application.name + "." + self.name)
        signals.post_init.send(self.__class__, instance=self)

    def help(self, out=None, initial_heading_level=0):
        """Return full reStructuredText help for this class"""
        builder = help.SchemaHelpBuilder(self.schema, self.url, out=out, initial_heading_level=initial_heading_level)
        builder.begin_subheading(self.name)
        builder.begin_list()
        builder.define("Application", self.application.url + ';help')
        builder.define("Schema URL", self.schema_url)
        builder.define("JSON URL", self.url)
        builder.define("Primary Key", self.primary_key)
        builder.define("Anonymous Reads", "yes" if self.anonymous_reads else "no")
        builder.end_list()
        builder.end_subheading()
        builder.build()
        if self.exposed_methods:
            builder.begin_subheading("Methods")
            for name, method in self.exposed_methods.items():
                new_builder = help.SchemaHelpBuilder(method.schema(getattr(self, method.__name__)), initial_heading_level=builder._heading_level)
                new_builder.build()
                builder.line(new_builder.rst)
            builder.end_subheading()
        if self.document_class.exposed_methods:
            builder.begin_subheading("Document Instance Methods")
            for name, method in self.document_class.exposed_methods.items():
                new_builder = help.SchemaHelpBuilder(method.schema(getattr(self.document_class, method.__name__)), initial_heading_level=builder._heading_level)
                new_builder.build()
                builder.line(new_builder.rst)
            builder.end_subheading()

        return builder.rst

    def create_table(self, *args, **kwargs):
        """Create the database table for this collection. Args and keyword args are sent along to the rethinkdb
        table_create function.  Sends pre_table_creation and post_table_creation signals.
        """
        signals.pre_table_creation.send(
            self.__class__, instance=self, table_name=self.name, db_name=self.application.db)

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

        signals.post_table_creation.send(
            self.__class__, instance=self, table_name=self.name, db_name=self.application.db)

    def drop_table(self):
        """Delete the database table for this collection. Sends pre_table_deletion and post_table_deletion signals.
        """

        signals.pre_table_deletion.send(
            self.__class__, instance=self, table_name=self.name, db_name=self.application.db)

        ret = r.db(self.application.db).table_drop(self.name).run(self.application.connection)
        self.log.info('Dropped table {0}.{1}'.format(self.application.db, self.name))

        signals.post_table_deletion.send(
            self.__class__, instance=self, table_name=self.name, db_name=self.application.db)

        return ret

    def _to_python_repr(self, doc):
        for property, special in self.specials.items():
            if property in doc:
                doc[property] = special.to_python_repr(doc[property])

    def _to_json_repr(self, doc):
        for property, special in self.specials.items():
            if property in doc:
                doc[property] = special.to_json_repr(doc[property])

    def _to_rql_repr(self, doc):
        for property, special in self.specials.items():
            if property in doc:
                doc[property] = special.to_rql_repr(doc[property])

    def __getitem__(self, key):
        """Get an object from the database and populate an instance of self.document_class with its contents.

        Args:
            key (str or int): Primary key for the document.

        Returns:
            Document: An instance of self.document_class with data from the database.

        Raises:
            KeyError if the object is not found in the database.
        """
        doc = self.table.get(key).run(self.application.connection)
        if doc:
            self._to_python_repr(doc)
            return self.document_class(doc, collection=self)
        else:
            raise KeyError('{0} not found in {1}'.format(key, self.url))

    def __setitem__(self, key, value):
        """Add or replace a document object to the database.

        Sends pre- and post- save signals. See signals documentation for more details.

        Args:
            key (str or int): The primary key for the document.
            value: (dict or Document): If a Document, it should be this collection's document_class.
        """
        value[self.primary_key] = key
        return self.save(value, conflict='replace').run(self.application.connection)

    def __delitem__(self, key):
        """Delete an object from the database.

        Sends pre- and post- delete signals. See signals documentation for more details.

        Args:
            key (str or int): The primary key for the document.
        """
        doc_signals.pre_delete.send(self.document_class, key=key)
        results = self.table.get(key).delete().run(self.application.connection)
        doc_signals.post_delete.send(self.document_class, results=results)

    def __iter__(self):
        for doc in self.table.run(self.application.connection):
            self._to_python_repr(doc)
            yield doc

    def __contains__(self, item):
        """Checks to see if the primary key is in the database.

        Args:
            item (dict, Document, str, or int): If a dict or a document, then the primary key will be checked.  Str or
              ints are assumed to be the primary key.

        Returns:
            True or False.
        """

        if isinstance(item, dict) or isinstance(item, Document):
            key = item[self.primary_key]
        else:
            key = item

        doc = self.table.get(item).run(self.application.connection)
        return doc is not None

    def __len__(self):
        return self.table.count().run(self.application.connection)

    def q(self, query):
        """Perform a query on this collection's database connection.

        Args:
            query (ReQL): Should be a RethinkDB query that returns documents for this collection.

        Yields:
            Document instances.
        """
        for doc in query.run(self.application.connection):
            self._to_python_repr(doc)
            yield self.document_class(doc, collection=self)

    def doc(self, value):
        """Return a document instance populated from a dict. Does **not** save document before returning.

        Args:
            value (dict): The value to use for the document. Should conform to document_class's schema.

        Returns:
            Document instance.
        """
        return self.document_class(value, collection=self)

    def create(self, value):
        """Create a document from a dict. Saves document before returning, and thus also sends pre- and post- save
        signals.

        Args:
            value (dict): The value to use for the new document.

        Returns:
            Document instance, guaranteed to have been saved.
        """
        doc = self.document_class(value, collection=self)
        ret = self.save(doc, conflict="error")
        if 'generated_keys' in ret:
            doc.id = ret['generated_keys'][0]
        return doc

    def validator(self, value):
        """Override this method to do extra validation above and beyond a simple schema check.

        Args:
            value (Document): The value to validate.

        Returns:
            bool

        Raises:
            ValidationError if the document fails to validate.
        """
        return True

    def delete(self, docs, **kwargs):
        """Delete a document or list of documents from the database.

        Args:
            docs (Document or [Document] or [primary_key]): List of documents to delete.
            **kwargs: Passed to rethinkdb.delete

        Returns:
            The result of RethinkDB delete.
        """
        if not isinstance(docs, list):
            docs = [docs]

        values = [v.id if isinstance(v, Document) else v for v in docs]
        return self.table.get_all(*values).delete(**kwargs).run(self.application.connection)

    def save(self, docs, **kwargs):
        """Save a document or list of documents to the database.

        Args:
            docs (Document or [Document] or [dict]): List of documents to save.
            **kwargs: Passed to rethinkdb.save

        Returns:
            The result of the RethinkDB save.
        """
        if not isinstance(docs, list):
            docs = [docs]

        values = []
        doc_signals.pre_save.send(self.document_class, docs=docs)
        for value in docs:
            if isinstance(value, Document):
                value = copy(value.obj)

            value = references(value)  # get rid of Document objects and turn them into URLs
            self._to_json_repr(value)
            jsonschema.validate(value, self.schema)
            self.validator(value)

            self._to_rql_repr(value)
            values.append(value)

        ret = self.table.insert(values, **kwargs).run(self.application.connection)
        doc_signals.post_save.send(self.document_class, results=ret)

        return ret


class ValueHandler(object):
    """This is base class for transforming values to/from RethinkDB representations to standard representations.

    Attributes:
        is_geometry (bool): Does this handle geometry/geographical values. Indicates to Sondra that indexing should
            be handled differently.
    """
    is_geometry = False

    def to_rql_repr(self, value):
        """Transform the object value into a ReQL object for storage.

        Args:
            value: The value to transform

        Returns:
            object: A ReQL object.
        """
        return value

    def to_json_repr(self, value):
        """Transform the object from a ReQL value into a standard value.

        Args:
            value (ReQL): The value to transform

        Returns:
            dict: A Python object representing the value.
        """
        return value

    def to_python_repr(self, value):
        """Transform the object from a ReQL value into a standard value.

        Args:
            value (ReQL): The value to transform

        Returns:
            dict: A Python object representing the value.
        """
        return value


class DocumentProcessor(object):
    def is_necessary(self, changed_props):
        """Override this method to determine whether the processor should run."""
        return False

    def run(self, document):
        """Override this method to post-process a document after it has changed."""
        return document


class SlugPropertyProcessor(DocumentProcessor):
    def __init__(self, source_prop, dest_prop='slug'):
        self.dest_prop = dest_prop
        self.source_prop = source_prop

    def is_necessary(self, changed_props):
        return self.source_prop in changed_props

    def run(self, document):
        document[self.dest_prop] = slugify(document[self.source_prop])


class Geometry(ValueHandler):
    """A value handler for GeoJSON"""
    is_geometry = True

    def __init__(self, *allowed_types):
        self.allowed_types = set(x.lower() for x in allowed_types) if allowed_types else None

    def to_rql_repr(self, value):
        if self.allowed_types:
            if value['type'].lower() not in self.allowed_types:
                raise ValidationError('value not in ' + ','.join(t for t in self.allowed_types))
        return r.geojson(value)

    def to_json_repr(self, value):
        if isinstance(value, BaseGeometry):
            return mapping(value)
        else:
            return value

    def to_python_repr(self, value):
        del value['$reql_type$']
        return shape(value)


class Time(ValueHandler):
    """A value handler for Python datetimes"""

    DEFAULT_TIMEZONE='Z'
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
            return r.iso8601(value, default_timezone=self.DEFAULT_TIMEZONE).in_timezone(self.timezone)
        elif isinstance(value, int) or isinstance(value, float):
            return datetime.fromtimestamp(value).isoformat()
        elif isinstance(value, dict):
            return r.time(
                value.get('year', None),
                value.get('month', None),
                value.get('day', None),
                value.get('hour', None),
                value.get('minute', None),
                value.get('second', None),
                value.get('timezone', self.timezone),
            ).in_timezone(self.timezone)
        else:
            return r.iso8601(value.isoformat(), default_timezone=self.DEFAULT_TIMEZONE).as_timezone(self.timezone)

    def to_json_repr(self, value):
        if isinstance(value, date) or isinstance(value, datetime):
            return value.isoformat()
        elif hasattr(value, 'to_epoch_time'):
            return value.to_iso8601()
        elif isinstance(value, int) or isinstance(value, float):
            return datetime.fromtimestamp(value).isoformat()
        else:
            return value

    def to_python_repr(self, value):
        if isinstance(value, str):
            return iso8601.parse_date(value)
        elif isinstance(value, datetime):
            return value
        elif hasattr(value, 'to_epoch_time'):
            timestamp = value.to_epoch_time()
            tz = value.timezone()
            offset = self.from_rql_tz(tz)
            offset_tz = timezone(offset)
            dt = datetime.fromtimestamp(timestamp, tz=offset_tz)
            return dt