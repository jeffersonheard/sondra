"""Core data document types.
"""

from collections.abc import MutableMapping
from functools import partial
from shapely.geometry import mapping, shape
from shapely.geometry.base import BaseGeometry
import json
import jsonschema
import rethinkdb as r
from datetime import date, datetime, timezone
import iso8601

from sondra import utils, Suite


def _to_ref(doc):
    if isinstance(doc, Document):
        return doc.url
    else:
        return doc

def _from_ref(doc):
    env = Suite()
    if isinstance(doc, str):
        if doc.startswith(env.base_url):
            return env.lookup_document(doc)
        else:
            return doc
    else:
        return doc


references = partial(utils.mapjson, _to_ref)


documents = partial(utils.mapjson, _from_ref)



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
    DEFAULT_TIMEZONE='Z'
    """A valuehandler for Python datetimes"""
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
    def application(self):
        return self.collection.application

    @property
    def id(self):
        return self.obj.get(self.collection.primary_key, None)

    @id.setter
    def id(self, v):
        self.obj[self.collection.primary_key] = v
        self.url = '/'.join((self.collection.url, v))

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

