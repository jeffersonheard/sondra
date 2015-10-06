"""Core data document types.
"""
from abc import ABCMeta
from collections.abc import MutableMapping
from datetime import date, datetime, timezone
from functools import partial
import io
import json

import iso8601
import jsonschema
import rethinkdb as r
from shapely.geometry import mapping, shape
from shapely.geometry.base import BaseGeometry

from sondra import utils, help
from sondra.utils import mapjson


def _to_ref(doc):
    if isinstance(doc, Document):
        return doc.url
    else:
        return doc

references = partial(utils.mapjson, _to_ref)


class ValidationError(Exception):
    """This kind of validation error is thrown whenever an :class:`Application` or :class:`Collection` is
    misconfigured."""


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


class DocumentMetaclass(ABCMeta):
    exposed_methods = {}
    schema = {
        "type": "object",
        "properties": {}
    }

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
        super(DocumentMetaclass, cls).__init__(name, bases, nmspc)

        for name, method in (n for n in nmspc.items() if hasattr(n[1], 'exposed')):
            cls.exposed_methods['name'] = method

        if 'description' not in cls.schema and cls.__doc__:
            cls.schema['description'] = cls.__doc__

        cls.defaults = {k: cls.schema['properties'][k]['default']
                        for k in cls.schema['properties']
                        if 'default' in cls.schema['properties'][k]}


class Document(MutableMapping, metaclass=DocumentMetaclass):
    def __init__(self, obj, collection=None, parent=None):
        self.collection = collection
        if self.collection:
            self.schema = self.collection.schema  # this means it's only calculated once. helpful.
        else:
            self.schema = mapjson(lambda x: x(context=x) if callable(x) else x, self.schema)  # turn URL references into URLs

        # todo add methods to the schema.

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

    def __delitem__(self, key):
        del self.obj[key]

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

