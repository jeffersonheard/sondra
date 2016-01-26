"""Core data document types.
"""
import json
import logging

from abc import ABCMeta
from collections.abc import MutableMapping
from copy import deepcopy
from datetime import datetime, date

import iso8601
import jsonschema
import rethinkdb as r
from slugify import slugify

from sondra.exceptions import ValidationError
from sondra.expose import method_schema

try:
    from shapely.geometry import mapping, shape
    from shapely.geometry.base import BaseGeometry
except:
    logging.warning("Shapely not imported. Geometry objects will not be supported directly.")

from sondra import utils, help
from sondra.utils import mapjson, split_camelcase
from sondra.schema import merge
from sondra.ref import Reference

__all__ = (
    "Document",
    "DocumentMetaclass"
)


def _reference(v):
    if isinstance(v, Document):
        if not v.id:
            v.save()
        return v.url
    else:
        return v


class DocumentMetaclass(ABCMeta):
    """
    The metaclass for all documents merges definitions and schema into a single schema attribute and makes sure that
    exposed methods are catalogued.
    """
    def __new__(mcs, name, bases, attrs):
        definitions = {}
        schema = attrs.get('schema', {"type": "object", "properties": {}})

        for base in bases:  # make sure this class inherits definitions and schemas
            if hasattr(base, "definitions") and base.definitions is not None:
                definitions = merge(deepcopy(base.definitions), definitions)
            if hasattr(base, "schema") and base.schema is not None:
                schema = merge(deepcopy(base.schema), schema)

        if "definitions" in attrs:
            merge(attrs['definitions'], definitions)
        else:
            attrs['definitions'] = definitions

        if 'title' not in attrs or (attrs['title'] is None):
            if 'title' in schema:
                attrs['title'] = schema['title']
            else:
                attrs['title'] = split_camelcase(name)

        attrs['schema'] = schema
        attrs['schema']['title'] = attrs['title']

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
        cls.schema['template'] = nmspc.get('template','{id}')

        cls.defaults = {k: cls.schema['properties'][k]['default']
                        for k in cls.schema['properties']
                        if 'default' in cls.schema['properties'][k]}


class Document(MutableMapping, metaclass=DocumentMetaclass):
    """
    The base type of an individual RethinkDB record.

    Each record is an instance of exactly one document class. To combine schemas and object definitions, you can use
    Python inheritance normally.  Inherit from multiple Document classes to create one Document class whose schema and
    definitions are combined by reference.

    Most Document subclasses will define at the very least a docstring,

    Attributes:
        collection (sondra.collection.Collection): The collection this document belongs to. FIXME could also use URL.
        defaults (dict): The list of default values for this document's properties.
        title (str): The title of the document schema. Defaults to the case-split name of the class.
        template (string): A template string for formatting documents for rendering.  Can be markdown.
        schema (dict): A JSON-serializable object that is the JSON schema of the document.
        definitions (dict): A JSON-serializable object that holds the schemas of all referenced object subtypes.
        exposed_methods (list): A list of method slugs of all the exposed methods in the document.
    """
    title = None
    defaults = {}
    template = "{id}"
    processors = []
    specials = {}

    def __init__(self, obj, collection=None, from_db=False):
        self.collection = collection
        self._saved = from_db

        if self.collection is not None:
            self.schema = self.collection.schema  # this means it's only calculated once. helpful.
        else:
            self.schema = mapjson(lambda x: x(context=self) if callable(x) else x, self.schema)  # turn URL references into URLs

        self._url = None
        if self.collection.primary_key in obj:
            self._url = '/'.join((self.collection.url, _reference(obj[self.collection.primary_key])))
        if '_url' in obj:
            del obj['_url']

        self.obj = {}
        if obj:
            for k, v in obj.items():
                self[k] = v

        for k in self.defaults:
            if k not in self:
                self[k] = self.defaults[k]

        for k, vh in self.specials.items():
            if k not in self:
                if vh.has_default:
                    self[k] = vh.default_value()

    def __str__(self):
        return str(self.obj)


    @property
    def application(self):
        """The application instance this document's collection is attached to."""
        return self.collection.application

    @property
    def suite(self):
        """The suite instance this document's application is attached to."""
        return self.application.suite

    @property
    def id(self):
        """The value of the primary key field. None if the value has not yet been saved."""
        if self._saved:
            return self.obj[self.collection.primary_key]
        else:
            return None

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
        """Included for symmetry with application and collection, the same as 'id'."""
        return self.id   # or self.UNSAVED

    def __len__(self):
        """The number of keys in the object"""
        return len(self.obj)

    def __eq__(self, other):
        """True if and only if the primary keys are the same"""
        return self.id and (self.id == other.id)

    def __getitem__(self, key):
        """Return either the value of the property or the default value of the property if the real value is undefined"""
        if isinstance(key, Document):  # handle the case where our primary key is a foreign key and the user passes in the instance.
            key = key.id

        if key in self.obj:
            v = self.obj[key]
        elif key in self.defaults:
            v = self.defaults[key]
        else:
            raise KeyError(key)

        if key in self.specials:
            return self.specials[key].to_python_repr(v, self)
        else:
            return v

    def __hash__(self):
        return hash(self.id)

    def fetch(self, key):
        """Return the value of the property interpreting it as a reference to another document"""
        if key in self.obj:
            if isinstance(self.obj[key], list):
                return [Reference(self.suite, ref).value for ref in self.obj[key]]
            elif isinstance(self.obj[key], dict):
                return {k: Reference(self.suite, ref).value for k, ref in self.obj[key].items()}
            if self.obj[key] is not None:
                return Reference(self.suite, self.obj[key]).value
            else:
                return None
        else:
            raise KeyError(key)

    def __setitem__(self, key, value):
        """Set the value of the property, saving it if it is an unsaved Document instance"""
        value = _reference(value)
        if isinstance(value, list) or isinstance(value, dict):
            value = mapjson(_reference, value)

        if key in self.specials:
            value = self.specials[key].to_json_repr(value, self)

        self.obj[key] = value

        for p in self.processors:
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
                new_builder = help.SchemaHelpBuilder(method_schema(self, method), initial_heading_level=builder._heading_level)
                new_builder.build()
                builder.line(new_builder.rst)

        return builder.rst

    def json(self, *args, **kwargs):
        return json.dumps(self.obj, *args, **kwargs)

    def rql_repr(self):
        ret = deepcopy(self.obj)
        valuehandlers = self.specials or {}
        for k, handler in valuehandlers.items():
            if k in ret:
                ret[k] = handler.to_rql_repr(ret[k], self)

        return ret

    def json_repr(self):
        return deepcopy(self.obj)

    def save(self, conflict='replace', *args, **kwargs):
        return self.collection.save(self, conflict=conflict, *args, **kwargs)

    def delete(self, **kwargs):
        return self.collection.delete(self, **kwargs)

    def validate(self):
        jsonschema.validate(self.obj, self.schema)



# class ValueHandler(object):
#     """This is base class for transforming values to/from RethinkDB representations to standard representations.
#
#     Attributes:
#         is_geometry (bool): Does this handle geometry/geographical values. Indicates to Sondra that indexing should
#             be handled differently.
#     """
#     is_geometry = False
#     has_default = False
#
#     def to_rql_repr(self, value):
#         """Transform the object value into a ReQL object for storage.
#
#         Args:
#             value: The value to transform
#
#         Returns:
#             object: A ReQL object.
#         """
#         return value
#
#     def to_json_repr(self, value):
#         """Transform the object from a ReQL value into a standard value.
#
#         Args:
#             value (ReQL): The value to transform
#
#         Returns:
#             dict: A Python object representing the value.
#         """
#         return value
#
#     def to_python_repr(self, value):
#         """Transform the object from a ReQL value into a standard value.
#
#         Args:
#             value (ReQL): The value to transform
#
#         Returns:
#             dict: A Python object representing the value.
#         """
#         return value
#
#     def default_value(self):
#         raise NotImplemented()
#
#
# # class Geometry(ValueHandler):
#     """A value handler for GeoJSON"""
#     is_geometry = True
#
#     def __init__(self, *allowed_types):
#         self.allowed_types = set(x.lower() for x in allowed_types) if allowed_types else None
#
#     def to_rql_repr(self, value):
#         if self.allowed_types:
#             if value['type'].lower() not in self.allowed_types:
#                 raise ValidationError('value not in ' + ','.join(t for t in self.allowed_types))
#         return r.geojson(value)
#
#     def to_json_repr(self, value):
#         if isinstance(value, BaseGeometry):
#             return mapping(value)
#         elif '$reql_type$' in value:
#             del value['$reql_type$']
#             return value
#         else:
#             return value
#
#     def to_python_repr(self, value):
#         if isinstance(value, BaseGeometry):
#             return value
#         if '$reql_type$' in value:
#             del value['$reql_type$']
#         return shape(value)
#
#
# class DateTime(ValueHandler):
#     """A value handler for Python datetimes"""
#
#     DEFAULT_TIMEZONE='Z'
#     def __init__(self, timezone='Z'):
#         self.timezone = timezone
#
#     def from_rql_tz(self, tz):
#         if tz == 'Z':
#             return 0
#         else:
#             posneg = -1 if tz[0] == '-' else 1
#             hours, minutes = map(int, tz.split(":"))
#             offset = posneg*(hours*60 + minutes)
#             return offset
#
#     def to_rql_repr(self, value):
#         if isinstance(value, str):
#             return r.iso8601(value, default_timezone=self.DEFAULT_TIMEZONE).in_timezone(self.timezone)
#         elif isinstance(value, int) or isinstance(value, float):
#             return datetime.fromtimestamp(value).isoformat()
#         elif isinstance(value, dict):
#             return r.time(
#                 value.get('year', None),
#                 value.get('month', None),
#                 value.get('day', None),
#                 value.get('hour', None),
#                 value.get('minute', None),
#                 value.get('second', None),
#                 value.get('timezone', self.timezone),
#             ).in_timezone(self.timezone)
#         else:
#             return r.iso8601(value.isoformat(), default_timezone=self.DEFAULT_TIMEZONE).as_timezone(self.timezone)
#
#     def to_json_repr(self, value):
#         if isinstance(value, date) or isinstance(value, datetime):
#             return value.isoformat()
#         elif isinstance(value, str):
#             return value
#         elif isinstance(value, int) or isinstance(value, float):
#             return datetime.fromtimestamp(value).isoformat()
#         else:
#             return value.to_iso8601()
#
#     def to_python_repr(self, value):
#         if isinstance(value, str):
#             return iso8601.parse_date(value)
#         elif isinstance(value, datetime):
#             return value
#         else:
#             return iso8601.parse_date(value.to_iso8601())
#
#
# class Now(DateTime):
#     """Return a timestamp for right now if the value is null."""
#     has_default = True
#
#     def from_rql_tz(self, tz):
#         return 0
#
#     def to_rql_repr(self, value):
#         value = value or datetime.utcnow()
#         return super(Now, self).to_rql_repr(value)
#
#     def to_json_repr(self, value):
#         value = value or datetime.utcnow()
#         return super(Now, self).to_json_repr(value)
#
#     def to_python_repr(self, value):
#         value = value or datetime.utcnow()
#         return super(Now, self).to_python_repr(value)
#
#     def default_value(self):
#         return datetime.utcnow()