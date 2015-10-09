from collections.abc import MutableMapping, Mapping
from abc import ABCMeta
from copy import deepcopy, copy
import jsonschema
import rethinkdb as r
import logging
import logging.config

from sondra import utils
from sondra.application import Application
from sondra.document import Document, references, signals as doc_signals

from . import signals
from sondra import help

_validator = jsonschema.Draft4Validator

class CollectionException(Exception):
    """Represents a misconfiguration in a :class:`Collection` class definition"""


class CollectionMetaclass(ABCMeta):

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

        if 'description' not in cls.schema:
            cls.schema['description'] = cls.__doc__ or "No description provided"

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

        cls.schema["methods"] = {m.slug: m.schema for m in cls.exposed_methods}
        cls.schema["documentMethods"] = {m.slug: m.schema for m in cls.document_class.exposed_methods}

        _validator.check_schema(cls.schema)

        if not hasattr(cls, 'application') or cls.application is None:
            raise CollectionException("{0} declared without application".format(name))
        elif not cls.abstract:
            cls.slug = utils.camelcase_slugify(cls.__name__)
            cls.application.register_collection(cls)
        else:
            pass


class Collection(MutableMapping, metaclass=CollectionMetaclass):
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
        return builder.rst

    def create_table(self, *args, **kwargs):
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
        doc = self.table.get(key).run(self.application.connection)
        if doc:
            self._to_python_repr(doc)
            return self.document_class(doc, collection=self)
        else:
            raise KeyError('{0} not found in {1}'.format(key, self.url))

    def __setitem__(self, key, value):
        return self.save(value, conflict='replace').run(self.application.connection)

    def __delitem__(self, key):
        doc_signals.pre_delete.send(self.document_class, key=key)
        results = self.table.get(key).delete().run(self.application.connection)
        doc_signals.post_delete.send(self.document_class, results=results)

    def __iter__(self):
        for doc in self.table.run(self.application.connection):
            self._to_python_repr(doc)
            yield doc

    def __contains__(self, item):
        doc = self.table.get(item).run(self.application.connection)
        return doc is not None

    def __len__(self):
        return self.table.count().run(self.application.connection)

    def q(self, query):
        for doc in query.run(self.application.connection):
            self._to_python_repr(doc)
            yield self.document_class(doc, collection=self)

    def doc(self, kwargs):
        return self.document_class(kwargs, collection=self)

    def create(self,kwargs):
        doc = self.document_class(kwargs, collection=self)
        ret = self.save(doc, conflict="error")
        if 'generated_keys' in ret:
            doc.id = ret['generated_keys'][0]
        return doc

    def validator(self, value):
        return True

    def delete(self, docs, **kwargs):
        if not isinstance(docs, list):
            docs = [docs]

        values = [v.id if isinstance(v, Document) else v for v in docs]
        return self.table.get_all(*values).delete(**kwargs).run(self.application.connection)

    def save(self, docs, **kwargs):
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

