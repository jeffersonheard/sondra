"""Core data document types.
"""
import json
import logging

from abc import ABCMeta
from collections.abc import MutableMapping
from functools import partial
import jsonschema

try:
    from shapely.geometry import mapping, shape
    from shapely.geometry.base import BaseGeometry
except:
    logging.warning("Shapely not imported. Geometry objects will not be supported.")

from sondra import utils, help
from sondra.utils import mapjson

__all__ = (
    "ValidationError",
    "Document",
    "DocumentMetaclass"
)

def _to_ref(doc):
    if isinstance(doc, Document):
        return doc.url
    else:
        return doc

references = partial(utils.mapjson, _to_ref)


class ValidationError(Exception):
    """This kind of validation error is thrown whenever an :class:`Application` or :class:`Collection` is
    misconfigured."""


class DocumentMetaclass(ABCMeta):
    def __new__(mcs, name, bases, attrs):
        definitions = {}
        schema = attrs.get('schema', {"type": "object", "properties": {}})

        for base in bases:  # make sure this class inherits definitions and schemas
            if hasattr(base, "definitions") and base.definitions:
                definitions.update(base.definitions)
            if hasattr(base, "collection"):
                if "allOf" not in schema:
                    schema["allOf"] = []
                schema['allOf'].append({"$ref": base.collection.schema_url})

        if "definitions" in attrs:
            attrs['definitions'].update(definitions)
        else:
            attrs['definitions'] = definitions

        attrs['schema'] = schema

        return super().__new__(mcs, name, bases, attrs)

    def __init__(cls, name, bases, nmspc):
        super(DocumentMetaclass, cls).__init__(name, bases, nmspc)
        cls.exposed_methods = {}

        for base in bases:
            if hasattr(base, 'exposed_methods'):
                cls.exposed_methods.update(base.exposed_methods)

        for name, method in (n for n in nmspc.items() if hasattr(n[1], 'exposed')):
                cls.exposed_methods[name] = method

        if 'description' not in cls.schema and cls.__doc__:
            cls.schema['description'] = cls.__doc__

        cls.schema['methods'] = [m.slug for m in cls.exposed_methods.values()]
        cls.schema['definitions'] = nmspc.get('definitions', {})

        cls.defaults = {k: cls.schema['properties'][k]['default']
                        for k in cls.schema['properties']
                        if 'default' in cls.schema['properties'][k]}


class Document(MutableMapping, metaclass=DocumentMetaclass):
    def __init__(self, obj, collection=None, parent=None):
        self.collection = collection
        if self.collection:
            self.schema = self.collection.schema  # this means it's only calculated once. helpful.
        else:
            self.schema = mapjson(lambda x: x(context=self) if callable(x) else x, self.schema)  # turn URL references into URLs

        if not self.collection and (parent and parent.collection):
            self.collection = parent.collection

        self.parent = parent

        self._url = None
        if self.collection.primary_key in obj:
            self._url = '/'.join((self.collection.url, obj[self.collection.primary_key]))

        self._referenced = True
        self.obj = {}
        if obj:
            for k, v in obj.items():
                self[k] = v

    @property
    def application(self):
        return self.collection.application

    @property
    def suite(self):
        return self.application.suite

    @property
    def id(self):
        return self.obj.get(self.collection.primary_key, None)

    @id.setter
    def id(self, v):
        self.obj[self.collection.primary_key] = v
        self._url = '/'.join((self.collection.url, v))

    @property
    def name(self):
        return self.id or "<unsaved>"

    @property
    def url(self):
        if self._url:
            return self._url
        elif self.collection:
            return self.collection.url + "/" + self.slug
        else:
            return self.slug

    @property
    def schema_url(self):
        return self.url + ";schema"

    @property
    def slug(self):
        return self.id or "<unsaved>"

    def __len__(self):
        return len(self.obj)

    def __eq__(self, other):
        return self.id and (self.id == other.id)

    def __getitem__(self, key):
        if key in self.obj:
            return self.obj[key]
        elif key in self.defaults:
            return self.defaults[key]
        else:
            raise KeyError(key)

    def __setitem__(self, key, value):
        if isinstance(value, Document):
            value.parent = self
            self.referenced = False
        self.obj[key] = value
        if self.collection:
            for p in self.collection.processors:
                if p.is_necessary(key):
                    p.run(self.obj)

    def __delitem__(self, key):
        del self.obj[key]
        if self.collection:
            for p in self.collection.processors:
                if p.is_necessary(key):
                    p.run(self.obj)

    def __iter__(self):
        return iter(self.obj)

    def help(self, out=None, initial_heading_level=0):
        """Return full reStructuredText help for this class"""
        builder = help.SchemaHelpBuilder(self.schema, self.url, out=out, initial_heading_level=initial_heading_level)
        builder.begin_subheading(self.name)
        builder.begin_list()
        builder.define("Collection", self.collection.url + ';help')
        builder.define("Schema URL", self.schema_url)
        builder.define("JSON URL", self.url)
        builder.end_list()
        builder.end_subheading()
        builder.build()
        if self.exposed_methods:
            builder.begin_subheading("Methods")
            for name, method in self.exposed_methods.items():
                new_builder = help.SchemaHelpBuilder(method.schema(getattr(self, method.__name__)), initial_heading_level=builder._heading_level)
                new_builder.build()
                builder.line(new_builder.rst)


        return builder.rst

    def json(self, *args, **kwargs):
        if not self._referenced:
            self.reference()
        return json.dumps(self.obj, *args, **kwargs)

    def save(self, *args, **kwargs):
        return self.collection.save(self.obj, *args, **kwargs)

    def delete(self, **kwargs):
        return  self.collection.delete(self.id, **kwargs)

    def _from_ref(self, doc):
        if isinstance(doc, str):
            if doc.startswith(self.suite.base_url):
                return self.suite.lookup_document(doc)
            else:
                return doc
        else:
            return doc

    def dereference(self):
        self.obj = utils.mapjson(self._from_ref, self.obj)
        self._referenced = False
        return self

    def reference(self):
        self.obj = references(self.obj)
        self._referenced = True
        return self

    def validate(self):
        jsonschema.validate(self.obj, self.schema)

