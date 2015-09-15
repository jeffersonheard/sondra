from collections.abc import MutableMapping, Mapping
from abc import ABCMeta
from copy import deepcopy, copy
import jsonschema
import rethinkdb as r
import logging
import logging.config

from sondra import utils, Document, Application
from sondra.document import references, signals as doc_signals

from . import signals


_validator = jsonschema.Draft4Validator

class CollectionException(Exception):
    """Represents a misconfiguration in a :class:`Collection` class definition"""


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

    @property
    def table(self):
        return r.db(self.application.db).table(self.name)

    def __init__(self, application):
        signals.pre_init.send(self.__class__, application)
        self.application = application
        self.url = '/'.join((self.application.url, self.slug))
        self.schema['id'] = self.url + ";schema"
        self.log = logging.getLogger(self.application.name + "." + self.name)
        signals.post_init.send(self.__class__, application)

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
        doc_signals.post_deleete.send(self.document_class, results=results)

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

