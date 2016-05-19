from datetime import datetime, date
import iso8601
import rethinkdb as r
from shapely.geometry.base import BaseGeometry
from shapely.geometry import mapping, shape

from sondra.exceptions import ValidationError
from sondra.ref import Reference
from sondra.utils import import_string


class ValueHandler(object):
    """This is base class for transforming values to/from RethinkDB representations to standard representations.

    Attributes:
        is_geometry (bool): Does this handle geometry/geographical values. Indicates to Sondra that indexing should
            be handled differently.
    """
    is_geometry = False
    has_default = False

    def to_rql_repr(self, value, document):
        """Transform the object value into a ReQL object for storage.

        Args:
            value: The value to transform

        Returns:
            object: A ReQL object.
        """
        return value

    def to_json_repr(self, value, document):
        """Transform the object from a ReQL value into a standard value.

        Args:
            value (ReQL): The value to transform

        Returns:
            dict: A Python object representing the value.
        """
        return value

    def to_python_repr(self, value, document):
        """Transform the object from a ReQL value into a standard value.

        Args:
            value (ReQL): The value to transform

        Returns:
            dict: A Python object representing the value.
        """
        return value

    def default_value(self):
        raise NotImplemented()

    def post_save(self, document):
        pass

    def pre_delete(self, document):
        pass


class ListHandler(ValueHandler):
    def __init__(self, sub_handler):
        self.sub_handler = sub_handler

    def to_rql_repr(self, value, document):
        if value is not None:
            return [self.sub_handler.to_rql_repr(v, document) for v in value]


    def to_json_repr(self, value, document):
        if value is not None:
            return [self.sub_handler.to_json_repr(v, document) for v in value]

    def to_python_repr(self, value, document):
        if value is not None:
            return [self.sub_handler.to_python_repr(v, document) for v in value]


class KeyValueHandler(ValueHandler):
    """Handle a key-value object of a particular type of value"""
    def __init__(self, sub_handler):
        self.sub_handler = sub_handler

    def to_rql_repr(self, value, document):
        if value:
            return {k: self.sub_handler.to_rql_repr(v, document) for k, v in value.items()}

    def to_json_repr(self, value, document):
        if value:
            return {k: self.sub_handler.to_json_repr(v, document) for k, v in value.items()}

    def to_python_repr(self, value, document):
        if value:
            return {k: self.sub_handler.to_python_repr(v, document) for k, v in value.items()}


class ForeignKey(ValueHandler):
    def __init__(self, app, coll, urlify=True):
        self._app = app
        self._coll = coll
        self._urlify = urlify

    def to_rql_repr(self, value, document):
        """Should be just the 'id' of the document, not the full URL for portability"""

        if value is None:
            return value
        elif isinstance(value, str):
            if value.startswith('/') or value.startswith('http'):
                return Reference(document.suite, value).value.id
            else:
                return value
        else:
            return value.id

    def to_json_repr(self, value, document):
        """The URL of the document"""

        if value is None:
            return None
        elif isinstance(value, str):
            if value.startswith('/') or value.startswith('http'):
                return value
            elif self._urlify:
                return document.suite[self._app][self._coll][value].url
            else:
                return value
        elif self._urlify:
            return value.url
        else:
            return value.id

    def to_python_repr(self, value, document):
        """The document itself"""

        if value is None:
            return None
        elif isinstance(value, str):
            if value.startswith('/') or value.startswith('http'):
                return Reference(document.suite, value).value
            else:
                return document.suite[self._app][self._coll][value]
        else:
            return value


class Geometry(ValueHandler):
    """A value handler for GeoJSON"""
    is_geometry = True

    def __init__(self, *allowed_types):
        self.allowed_types = set(x.lower() for x in allowed_types) if allowed_types else None

    def to_rql_repr(self, value, document):
        if value is None:
            return value

        if self.allowed_types:
            if value['type'].lower() not in self.allowed_types:
                raise ValidationError('value not in ' + ','.join(t for t in self.allowed_types))
        return r.geojson(value)

    def to_json_repr(self, value, document):
        if value is None:
            return value

        if isinstance(value, BaseGeometry):
            return mapping(value)
        elif '$reql_type$' in value:
            del value['$reql_type$']
            return value
        else:
            return value

    def to_python_repr(self, value, document):
        if value is None:
            return value

        if isinstance(value, BaseGeometry):
            return value
        if '$reql_type$' in value:
            del value['$reql_type$']
        return value


class DateTime(ValueHandler):
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

    def to_rql_repr(self, value, document):
        if value is None:
            return value

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

    def to_json_repr(self, value, document):
        if value is None:
            return value

        if isinstance(value, date) or isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, str):
            return value
        elif isinstance(value, int) or isinstance(value, float):
            return datetime.fromtimestamp(value).isoformat()
        else:
            return value.to_iso8601()

    def to_python_repr(self, value, document):
        if value is None:
            return value

        if isinstance(value, str):
            return iso8601.parse_date(value)
        elif isinstance(value, datetime):
            return value
        else:
            return iso8601.parse_date(value.to_iso8601())



class Now(DateTime):
    """Return a timestamp for right now if the value is null."""
    has_default = True

    def from_rql_tz(self, tz):
        return 0

    def to_rql_repr(self, value, document):
        value = value or datetime.utcnow()
        return super(Now, self).to_rql_repr(value, document)

    def to_json_repr(self, value, document):
        value = value or datetime.utcnow()
        return super(Now, self).to_json_repr(value, document)

    def to_python_repr(self, value, document):
        value = value or datetime.utcnow()
        return super(Now, self).to_python_repr(value, document)

    def default_value(self):
        return