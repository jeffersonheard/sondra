from collections import OrderedDict
from collections.abc import MutableMapping
from abc import ABCMeta
from copy import deepcopy, copy
import jsonschema
import rethinkdb as r
import logging
import logging.config

from sondra import help, utils
from sondra.document import Document, signals as doc_signals
from sondra.expose import method_schema, expose_method, expose_method_explicit
from . import signals
from sondra.utils import mapjson, resolve_class, split_camelcase

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
    document_class=None
    primary_key='id'

    def __new__(mcs, name, bases, attrs):
        definitions = {}
        for base in bases:
            if hasattr(base, "definitions") and base.definitions:
                definitions.update(base.definitions)

        if "definitions" in attrs:
            attrs['definitions'].update(definitions)
        else:
            attrs['definitions'] = definitions

        if 'document_class' in attrs:
            if isinstance(attrs['document_class'], str):
                attrs['document_class'] = resolve_class(attrs['document_class'], required_superclass=Document)

        return super().__new__(mcs, name, bases, attrs)

    def __init__(cls, name, bases, nmspc):
        super(CollectionMetaclass, cls).__init__(name, bases, nmspc)
        cls.exposed_methods = {}
        cls.title = cls.title or cls.__name__
        cls.name = utils.convert_camelcase(cls.__name__)

        for base in bases:
            if hasattr(base, 'exposed_methods'):
                cls.exposed_methods.update(base.exposed_methods)
        for name, method in (n for n in nmspc.items() if hasattr(n[1], 'exposed')):
                cls.exposed_methods[name] = method

        if cls.document_class and (cls.document_class is not Document):
            cls.abstract = False
            cls.schema = deepcopy(cls.document_class.schema)

            if not cls.primary_key:
                cls.schema['properties']['id'] = {"type": "string", "description": "The primary key.", "title": "ID"}

            cls.schema["methods"] = {}
            for m in cls.exposed_methods.values():
                cls.schema['methods'][m.slug] = method_schema(False, m)

            print("Cataloging document methods")
            cls.schema['documentMethods'] = {}
            for m in cls.document_class.exposed_methods.values():
                cls.schema["documentMethods"][m.slug] = method_schema(None, m)
            print("End catalog step")

            if not 'description' in cls.schema:
                cls.schema['description'] = cls.document_class.__doc__ or 'No Description Provided.'

            if not 'title' in cls.schema:
                cls.schema['title'] = split_camelcase(cls.document_class.__name__)

            _validator.check_schema(cls.schema)

            cls.slug = utils.camelcase_slugify(cls.__name__)

        else:
            cls.abstract = True


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

    title = None
    name = None
    slug = None
    schema = None
    application = None
    exposed_methods = None
    file_storage = None
    document_class = Document
    primary_key = "id"
    private = False
    indexes = []
    relations = []
    anonymous_reads = True
    abstract = False
    autocomplete_props = None
    order_by = None
    order_by_index = None

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
        if self.abstract:
            raise CollectionException("Tried to instantiate an abstract collection")

        signals.pre_init.send(self.__class__, instance=self)
        self.title = self.document_class.title
        self.application = application
        self._url = '/'.join((self.application.url, self.slug))
        self.schema['id'] = self.url + ";schema"
        self.schema = mapjson(lambda x: x(context=self.application.suite) if callable(x) else x, self.schema)
        self.log = logging.getLogger(self.application.name + "." + self.name)

        if self.autocomplete_props is None:
            self.autocomplete_props = (self.primary_key,)

        if self.file_storage:
            self.file_storage = self.file_storage(self)
        signals.post_init.send(self.__class__, instance=self)

    def __str__(self):
        return self.url

    def help(self, out=None, initial_heading_level=0):
        """Return full reStructuredText help for this class"""
        builder = help.SchemaHelpBuilder(self.schema, self.url, out=out, initial_heading_level=initial_heading_level)
        builder.build()
        builder.begin_subheading("Info")
        builder.begin_list()
        builder.define("Application", self.application.url + ';help')
        builder.define("Schema URL", self.schema_url)
        builder.define("JSON URL", self.url)
        builder.define("Primary Key", self.primary_key)
        builder.end_list()
        builder.end_subheading()

        if self.exposed_methods:
            builder.begin_subheading("Methods")
            for name, method in self.exposed_methods.items():
                new_builder = help.SchemaHelpBuilder(method_schema(self, method), initial_heading_level=builder._heading_level)
                new_builder.build()
                builder.line(new_builder.rst)
            builder.end_subheading()
        if self.document_class.exposed_methods:
            builder.begin_subheading("Document Instance Methods")
            for name, method in self.document_class.exposed_methods.items():
                new_builder = help.SchemaHelpBuilder(method_schema(None, method), initial_heading_level=builder._heading_level)
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

            if index in self.document_class.specials and self.document_class.specials[index].is_geometry:
                geo = True
            else:
                geo = False

            try:
                if index_function:
                    self.table.index_create(index, index_function, multi=multi, geo=geo).run(self.application.connection)
                else:
                    self.table.index_create(index, multi=multi, geo=geo).run(self.application.connection)
            except r.ReqlError as e:
                self.log.info('Index {2} on table {0}.{1} already exists.'.format(self.application.db, self.name, index))

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

    def __hash__(self):
        return hash(self.name)

    def __getitem__(self, key):
        """Get an object from the database and populate an instance of self.document_class with its contents.

        Args:
            key (str or int): Primary key for the document.

        Returns:
            Document: An instance of self.document_class with data from the database.

        Raises:
            KeyError if the object is not found in the database.
        """
        if isinstance(key, Document):  # handle the case where our primary key is a foreign key and the user passes in the instance.
            key = key.id

        doc = self.table.get(key).run(self.application.connection)
        if doc:
            return self.document_class(doc, collection=self, from_db=True)
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
        query = self.apply_ordering(self.table).get_field(self.primary_key)
        for k in query.run(self.application.connection):
            yield k

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

        doc = self.table.get(key).run(self.application.connection)
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
            if 'doc' in doc:
                doc = doc['doc']  # some queries return results that encapsulate the document with metadata
            yield self.document_class(doc, collection=self, from_db=True)

    def apply_ordering(self, query):
        if self.order_by_index and self.order_by:
            return query.order_by(index=self.order_by_index, *self.order_by)
        elif self.order_by_index:
            return query.order_by(index=self.order_by_index)
        elif self.order_by:
            return query.order_by(*self.order_by)
        else:
            return query

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

        if isinstance(value, list):
            docs = [(self.document_class(v, collection=self) if not isinstance(v, self.document_class) else v) for v in value]
        else:
            docs = [self.document_class(value, collection=self)]

        ret = self.save(docs, conflict="error")
        if 'generated_keys' in ret:
            for i, k in enumerate(ret['generated_keys']):
                docs[i].id = k

        if isinstance(value, list):
            return docs
        else:
            return docs[0]

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

    def delete(self, docs=None, **kwargs):
        """Delete a document or list of documents from the database.

        Args:
            docs (Document or [Document] or [primary_key]): List of documents to delete.
            **kwargs: Passed to rethinkdb.delete

        Returns:
            The result of RethinkDB delete.
        """
        if not docs:
            return self.table.delete(**kwargs).run(self.application.connection)

        if not isinstance(docs, list):
            docs = [docs]

        for value in docs:
            if isinstance(value, Document):
                for s in value.specials.values():
                    s.pre_delete(value)
                value.pre_delete()
        values = [v.id if isinstance(v, Document) else v for v in docs]
        ret = self.table.get_all(*values).delete(**kwargs).run(self.application.connection)
        for value in docs:
            value.post_delete()
        return ret

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

        for doc in docs:
            if not isinstance(doc, Document):
                doc = self.document_class(doc)

            doc.pre_save()
            doc.validate()
            doc._saved = True
            rql = doc.rql_repr()
            values.append(rql)

        ret = self.table.insert(values, **kwargs).run(self.application.connection)
        if docs and isinstance(docs[0], Document):
            for i, doc in enumerate(docs):
                doc._saved = True
                if 'generated_keys' in ret:
                    doc.id = ret['generated_keys'][i]
                    for s in doc.specials.values():
                        s.post_save(doc)
                doc.post_save()
                doc_signals.post_save.send(self.document_class, instance=doc)

        return ret

    def json_repr(self, docs, ordered=False, bare_keys=False):
        pop = False
        if not isinstance(docs, list):
            docs = [docs]
            pop = True

        values = [v.json_repr(ordered, bare_keys) for v in docs]

        if pop:
            return values[0]
        else:
            return values

    def geojson_repr(self, docs, ordered=False, bare_keys=False):
        pop = False
        if not isinstance(docs, list):
            docs = [docs]
            pop = True

        values = []
        for value in docs:
            if isinstance(value, Document):
                v = copy(value.obj)
                v['_url'] = value.url
                value = v

            values.append(value.geojson_repr(ordered, bare_keys))

        if pop:
            return values[0]
        else:
            return {"type": "FeatureCollection", "features": values}

    @expose_method_explicit(
        title='Object Count',
        side_effects=False,
        request_schema={"type": "null"},
        response_schema={"type": "object", "properties": {"_": {"type": "number"}}}
    )
    def count(self):
        """The number of objects in the collection."""
        return len(self)

    @expose_method_explicit(
        title='Autocomplete',
        side_effects=False,
        request_schema={
            "type": "object",
            "properties": {
                "partial": {
                    "type": "string",
                    "title": "Partial",
                    "description": "A regular expression to match on any and all of the autocomplete fields."
                }
            }
        },
        response_schema={
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "k": {"type": "string"},
                    "v": {"type": "string"}
                }
            }
        }
    )
    def autocomplete(self, partial, limit=25):
        """Search results based on a partial input as a regex"""
        tpl = self.document_class.template.replace('$', '')  # switch back to Python format syntax.
        partial = '(?i)' + partial
        result = {}
        reached_limit = False
        pk = self.primary_key  #
        for p in self.autocomplete_props:
            if reached_limit:
                break
            for obj in self.table.filter(lambda x: x[p].match(partial)).run(self.application.connection):
                if obj[pk] not in result:
                    result[obj[pk]] = tpl.format(**obj)
                if limit and len(result) >= limit:
                    reached_limit = True
                    break
        return sorted([{"k": k, "v": v} for k, v in result.items()], key=lambda x: x['v'])

    @expose_method_explicit(
        title='Keys',
        side_effects=False,
        request_schema={"type": "null"},
        response_schema={"type": "array", "items": {"type": "string"}}
    )
    def key_list(self) -> list:
        return list(self.keys())

    @expose_method_explicit(
        title='Key Map',
        side_effects=False,
        request_schema={"type": "null"},
        response_schema={"type": "object"}
    )
    def key_map(self) -> dict:
        return {k: str(self[k]) for k in self}