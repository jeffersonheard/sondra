from datetime import datetime, date

import iso8601
import re
import rethinkdb as r
from shapely.geometry import mapping
from shapely.geometry.base import BaseGeometry

import sondra.document
from sondra.exceptions import ValidationError
from sondra.api.ref import Reference


class ValueHandler(object):
    """This is base class for transforming values to/from RethinkDB representations to standard representations.

    Attributes:
        is_geometry (bool): Does this handle geometry/geographical values. Indicates to Sondra that indexing should
            be handled differently.
    """
    is_geometry = False
    has_default = False

    def __init__(self, *args, **kwargs):
        self._str = self.__class__.__name__ + str(args) + str(kwargs)

    def __str__(self):
        return self._str

    def __repr__(self):
        return self._str

    def to_rql_repr(self, value, document):
        """Transform the object value into a ReQL object for storage.

        Args:
            value: The value to transform

        Returns:
            object: A ReQL object.
        """
        return value

    def to_json_repr(self, value, document, **kwargs):
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
        super(ListHandler, self).__init__(sub_handler)
        self.sub_handler = sub_handler

    def to_rql_repr(self, value, document):
        if value is not None:
            return [self.sub_handler.to_rql_repr(v, document) for v in value]


    def to_json_repr(self, value, document, **kwargs):
        if value is not None:
            return [self.sub_handler.to_json_repr(v, document, **kwargs) for v in value]

    def to_python_repr(self, value, document):
        if value is not None:
            return [self.sub_handler.to_python_repr(v, document) for v in value]


class PropertyHandler(ValueHandler):
    def __init__(self, prop, sub_handler):
        super(PropertyHandler, self).__init__(sub_handler)
        self.prop = prop
        self.sub_handler = sub_handler

    def to_rql_repr(self, value, document):
        if value is None:
            return None

        if self.prop in value:
            v = dict(value)
            v[self.prop] = self.sub_handler.to_rql_repr(v[self.prop], document)
            return v
        else:
            return value

    def to_json_repr(self, value, document, **kwargs):
        if value is None:
            return None

        if self.prop in value:
            v = dict(value)
            v[self.prop] = self.sub_handler.to_json_repr(v[self.prop], document, **kwargs)
            return v
        else:
            return value

    def to_python_repr(self, value, document):
        if value is None:
            return None

        if self.prop in value:
            v = dict(value)
            v[self.prop] = self.sub_handler.to_python_repr(v[self.prop], document)
            return v
        else:
            return value


class PropertiesHandler(ValueHandler):
    def __init__(self, **handler_mapping):
        super(PropertiesHandler, self).__init__(**handler_mapping)
        self.sub_handlers = handler_mapping

    def to_rql_repr(self, value, document):
        if value is None:
            return None

        v = value
        for prop in self.sub_handlers:
            if prop in value:
                if v is value:  # don't clone until we have to make a modification
                    v = dict(value)

                v[prop] = self.sub_handlers[prop].to_rql_repr(v[prop], document)

        return v

    def to_json_repr(self, value, document, **kwargs):
        if value is None:
            return None

        v = value
        for prop in self.sub_handlers:
            if prop in value:
                if v is value:  # don't clone until we have to make a modification
                    v = dict(value)

                v[prop] = self.sub_handlers[prop].to_json_repr(v[prop], document, **kwargs)

        return v

    def to_python_repr(self, value, document):
        if value is None:
            return None

        v = value
        for prop in self.sub_handlers:
            if prop in value:
                if v is value:  # don't clone until we have to make a modification
                    v = dict(value)

                v[prop] = self.sub_handlers[prop].to_python_repr(v[prop], document)

        return v


class PatternPropertiesHandler(ValueHandler):
    def __init__(self, **handler_mapping):
        super(PatternPropertiesHandler, self).__init__(**handler_mapping)
        self.sub_handlers = handler_mapping

    def to_rql_repr(self, value, document):
        if value is None:
            return None

        v = value
        for pattern in self.sub_handlers:
            for k, v in value.items():
                if re.match(pattern, k):
                    if v is value:  # don't clone until we have to make a modification
                        v = dict(value)

                v[k] = self.sub_handlers[k].to_rql_repr(v[k], document)

        return v

    def to_json_repr(self, value, document, **kwargs):
        if value is None:
            return None

        v = value
        for pattern in self.sub_handlers:
            for k, v in value.items():
                if re.match(pattern, k):
                    if v is value:  # don't clone until we have to make a modification
                        v = dict(value)

                v[k] = self.sub_handlers[k].to_json_repr(v[k], document)

        return v

    def to_python_repr(self, value, document):
        v = value
        for pattern in self.sub_handlers:
            for k, v in value.items():
                if re.match(pattern, k):
                    if v is value:  # don't clone until we have to make a modification
                        v = dict(value)

                v[k] = self.sub_handlers[k].to_python_repr(v[k], document)

        return v


class KeyValueHandler(ValueHandler):
    """Handle a key-value object of a particular type of value"""
    def __init__(self, sub_handler):
        super(KeyValueHandler, self).__init__(sub_handler)
        self.sub_handler = sub_handler

    def to_rql_repr(self, value, document):
        if value:
            return {k: self.sub_handler.to_rql_repr(v, document) for k, v in value.items()}

    def to_json_repr(self, value, document, **kwargs):
        if value:
            return {k: self.sub_handler.to_json_repr(v, document) for k, v in value.items()}

    def to_python_repr(self, value, document):
        if value:
            return {k: self.sub_handler.to_python_repr(v, document) for k, v in value.items()}


class ForeignKey(ValueHandler):
    def __init__(self, app, coll, urlify=True):
        super(ForeignKey, self).__init__(app, coll, urlify=urlify)
        self.app = app
        self.coll = coll
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

    def to_json_repr(self, value, document, **kwargs):
        """The URL of the document"""
        if value is None:
            return None
        elif isinstance(value, str):
            if value.startswith('/') or value.startswith('http'):
                return value
            elif self._urlify and not kwargs.get('bare_keys', False):
                return document.suite[self.app][self.coll].url + '/' + value
            else:
                return value
        elif self._urlify and not kwargs.get('bare_keys', False):
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
                return document.suite[self.app][self.coll][value]
        else:
            return value


class Geometry(ValueHandler):
    """A value handler for GeoJSON"""
    is_geometry = True

    def __init__(self, *allowed_types):
        super(Geometry, self).__init__(*allowed_types)
        self.allowed_types = set(x.lower() for x in allowed_types) if allowed_types else None

    def to_rql_repr(self, value, document):
        if value is None:
            return value
        if value == {}:
            return None

        if self.allowed_types:
            if value['type'].lower() not in self.allowed_types:
                raise ValidationError('value not in ' + ','.join(t for t in self.allowed_types))
        return r.geojson(value)

    def to_json_repr(self, value, document, **kwargs):
        if value is None:
            return value
        if value == {}:
            return None

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
        if value == {}:
            return None

        if isinstance(value, BaseGeometry):
            return value
        if '$reql_type$' in value:
            del value['$reql_type$']
        return value


class DateTime(ValueHandler):
    """A value handler for Python datetimes"""

    DEFAULT_TIMEZONE='Z'
    def __init__(self, timezone='Z'):
        super(DateTime, self).__init__(timezone=timezone)
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

    def to_json_repr(self, value, document, **kwargs):
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

    def to_json_repr(self, value, document, **kwargs):
        value = value or datetime.utcnow()
        return super(Now, self).to_json_repr(value, document)

    def to_python_repr(self, value, document):
        value = value or datetime.utcnow()
        return super(Now, self).to_python_repr(value, document)

    def default_value(self):
        return


class SchemaParser(object):
    """
    Automatically determines value-handlers for the given document schema.
    """

    def __init__(self, schema, definitions):
        self._schema = schema
        self._definitions = definitions

    def __call__(self):
        return self._scan_properties_for_specials(self._schema)

    def _scan_properties_for_specials(self, s):
        if 'properties' in s:
            handlers = {pname: self._value_handler(pdef) for pname, pdef in s['properties'].items()}
            rem = []
            for k, v in handlers.items():
                if v is None:
                    rem.append(k)
            for k in rem:
                del handlers[k]
            return handlers
        else:
            return None

    def _scan_patternProperties_for_specials(self, s):
        if 'patternProperties' in s:
            handlers = {"[{0}]".format(pname): self._value_handler(pdef) for pname, pdef in s['patternProperties'].items()}
            rem = []
            for k, v in handlers.items():
                if v is None:
                    rem.append(k)
            for k in rem:
                del handlers[k]
            return handlers
        else:
            return None

    def _value_handler(self, pdef):
        ret = None
        ptype = pdef.get('type', 'string')

        if 'fk' in pdef:
            ret = ForeignKey(*pdef['fk'].split('/'))
        elif 'geo' in pdef:
            if 'geometry_type' in pdef:
                ret = Geometry(pdef['geometry_type'])
            else:
                ret = Geometry()
        elif 'formatters' in pdef:
            if pdef['formatters'] in {'time', 'datetime-local'}:
                ret = DateTime()
        elif 'format' in pdef:
            if pdef['format'] in {'date', 'time', 'date-time'}:
                ret = DateTime()
        elif '$ref' in pdef:
            defn_name = pdef['$ref'].rsplit('/', 1)[-1]
            if defn_name in self._definitions:
                defn = self._definitions[defn_name]
                defn_type = defn.get('type', 'string')  # technically wrong, but entirely reasonable. assume string because you can have a bare enum and it is almost always a string
                if defn_type != 'object':
                    ret = self._value_handler(defn)
                else:
                    handlers = self._scan_properties_for_specials(defn)
                    if handlers:
                        ret = PropertiesHandler(**handlers)
        elif ptype == 'object':
            handlers = self._scan_properties_for_specials(pdef)
            pattern_handlers = self._scan_patternProperties_for_specials(pdef)
            if handlers:
                ret = PropertiesHandler(**handlers)
            elif pattern_handlers:
                ret = PatternPropertiesHandler(**pattern_handlers)
        elif ptype == 'array':
            sub_handler = self._value_handler(pdef['items'])
            if sub_handler:
                ret = ListHandler(sub_handler)

        return ret