"""Core data document types.
"""
import json
import logging

from abc import ABCMeta
from collections import OrderedDict
from collections.abc import MutableMapping
from copy import deepcopy
import jsonschema
import heapq

from sondra.document.schema_parser import ListHandler, ForeignKey
from sondra.expose import method_schema, expose_method_explicit

try:
    from shapely.geometry import mapping, shape
    from shapely.geometry.base import BaseGeometry
except:
    logging.warning("Shapely not imported. Geometry objects will not be supported directly.")

from sondra import utils, help
from sondra.utils import mapjson, split_camelcase, natural_order, deprecated
from sondra.schema import S, merge
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
    This refactored metaclass does most of what the other metaclass did, but also looks through to find property setters
    and getters and pre-caches them.
    """
    def __new__(mcs, name, bases, attrs):
        definitions = {}
        schema = attrs.get('schema', S.object())

        # make sure this class inherits definitions and schemas
        for base in bases:
            if hasattr(base, "definitions") and base.definitions is not None:
                definitions = merge(deepcopy(base.definitions), definitions)
            if hasattr(base, "schema") and base.schema is not None:
                schema = merge(deepcopy(base.schema), schema)
            if hasattr(base, "__doc__"):
                docstring = base.__doc__

        # merge definitions from this class into the definition list.
        if "definitions" in attrs:
            merge(attrs['definitions'], definitions)
        else:
            attrs['definitions'] = definitions

        # set the title of the schema and class
        if 'title' not in attrs or (attrs['title'] is None):
            if 'title' in schema:
                attrs['title'] = schema['title']
            else:
                attrs['title'] = split_camelcase(name)

        # use the modified schema
        attrs['schema'] = schema

        return super().__new__(mcs, name, bases, attrs)

    def __init__(cls, name, bases, nmspc):
        cls.exposed_methods = {}

        # get a list of exposed methods. TODO do we really need to do this now, based on the decorator behavior?
        for base in bases:
            if hasattr(base, 'exposed_methods'):
                cls.exposed_methods.update(base.exposed_methods)

        for name, method in (n for n in nmspc.items() if hasattr(n[1], 'exposed')):
            cls.exposed_methods[name] = method

        # update schema
        cls.schema['methods'] = [m.slug for m in cls.exposed_methods.values()]
        cls.schema['definitions'] = nmspc.get('definitions', {})
        cls.schema['template'] = nmspc.get('template','{id}')  # set the expected behavior of __str__.

        # build the list of defaults from the schema
        cls.defaults = {k: cls.schema['properties'][k]['default']
                        for k in cls.schema['properties']
                        if 'default' in cls.schema['properties'][k]}

        super(DocumentMetaclass, cls).__init__(name, bases, nmspc)


class Document(MutableMapping, metaclass=DocumentMetaclass):
    """
    The base type of an persistent Document, corresponding to one RethinkDB record.

    Each record is an instance of exactly one document class. To combine schemas and object definitions, you can use
    Python inheritance normally.  Inherit from multiple Document classes to create one Document class whose schema and
    definitions are combined by reference.

    Most Document subclasses will define at the very least a docstring,

    Args:
        obj: a source Document or dict-like object
        collection (sondra.collections.Collection, optional): The Collection instance that this document belongs to, if any.
        from_db (bool=False): Set to true of this was constructed from a stored database object.
        metadata: Some kinds of queries return metadata about the object. If a db query returned metadata, it will be passed here.

    Attributes:
        collection (sondra.collection.Collection): The collection this document belongs to.
        defaults (dict): The list of default values for this document's properties.
        title (str): The title of the document schema. Defaults to the case-split name of the class.
        template (string): A template string for formatting documents for rendering.  Can be markdown.
        schema (dict): A JSON-serializable object that is the JSON schema of the document.
        definitions (dict): A JSON-serializable object that holds the schemas of all referenced object subtypes.
        exposed_methods (list): A list of method slugs of all the exposed methods in the document.
        saved (bool): if this document exists in the database.
        metadata (dict): A set of metadata from the database about this object (query-dependent)
        debug_validate_on_retrieval (bool=True): Set at the class derivation level. If when debugging, a validation
            step should happen when documents are retrieved from the database.
    """
    title = None
    defaults = {}
    template = "${id}"
    processors = []
    specials = {}
    store_nulls = set()
    debug_validate_on_retrieval = True

    def constructor(self, obj):
        """
        This is the document constructor.  It contains the business logic behind building the document
        from an input document or from the database.

        Args:
            obj: the obj passed in __init__
        """
        if self.collection.primary_key in obj:
            self._url = '/'.join((self.collection.url, _reference(obj[self.collection.primary_key])))

        if '_url' in obj:
            del obj['_url']

        if obj:
            for k, v in obj.items():
                try:
                    self[k] = v
                except Exception as e:
                    raise KeyError(k, str(e))

        for k in self.defaults:
            if k not in self:
                if callable(self.defaults[k]):
                    try:
                        self[k] = self.defaults[k]()
                    except:
                        self[k] = self.defaults[k](self.suite)
                else:
                    self[k] = self.defaults[k]

        for k, vh in self.specials.items():
            if k not in self:
                if vh.has_default:
                    self[k] = vh.default_value()

        for p in self.processors:
            p.run_on_constructor(self)

        if self.debug_validate_on_retrieval and self.saved and self.suite.debug:
            self.validate()

    def __init__(self, obj, collection=None, from_db=False, metadata=None):
        self.collection = collection
        self.saved = from_db
        self.metadata = metadata or {}
        self.obj = OrderedDict()

        if self.collection is not None:
            self.schema = self.collection.schema  # this means it's only calculated once. helpful.
        else:
            self.schema = mapjson(lambda x: x(context=self) if callable(x) else x, self.schema)  # turn URL references into URLs

        self._url = None
        self.constructor(obj)

    def __str__(self):
        return self.template.format(**self.obj)


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
        if self.saved:
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
        if isinstance(other, Document):
            return self.id and (self.id == other.id)
        elif isinstance(other, dict):
            return self.id and (self.id == other[self.collection.primary_key])
        else:
            return self.id == other

    def __contains__(self, item):
        return item in self.obj

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
        if value is None:
            if key not in self.store_nulls:
                if key in self.obj:
                    del self.obj[key]
            for p in self.processors:
                p.run_after_set(self, key)
        else:
            # value = _reference(value)
            # if isinstance(value, list) or isinstance(value, dict):
            #     value = mapjson(_reference, value)

            # if the key needs further processing, e.g. foreign keys, geometry, or dates, process.
            if key in self.specials:
                value = self.specials[key].to_json_repr(value, self, bare_keys=True)
                if value is None:
                    if key in self.obj:
                        del self.obj[key]
                        for p in self.processors:
                            p.run_after_set(self, key)
                    return

            # use the processed value as the value of the key.
            self.obj[key] = value

            # post-process the document after the value changes
            for p in self.processors:
                p.run_after_set(self, key)


    def __delitem__(self, key):
        del self.obj[key]
        for p in self.processors:
            p.run_after_set(self, key)

    def __iter__(self):
        return iter(self.obj)

    def update(*args, **kwargs):
        self, *args = args  # to conform to MutableMapping sig

        def sub_update(s, v, *k):
            if len(k) > 1:
                return sub_update(s[k[0]], v, k[1:])
            else:
                s[k[0]] = v
                return s

        for k, v in args:
            if '.' in k:
                k0, *ks = k.split('.')
                self[k0] = sub_update(self[k0], v, *ks)
            else:
                self[k] = v

        for k, v in kwargs.items():
            if '.' in k:
                k0, *ks = k.split('.')
                self[k0] = sub_update(self[k0], v, *ks)
            else:
                self[k] = v

        self.save()
        return self.collection[self.id]


    @expose_method_explicit(
        title='Related Documents',
        description='Reverse relation.  Get a query set of all documents in a collection that have foreign keys that '
                    'point to this document.',
        request_schema=S.object(
            required=['app','coll'],
            properties=S.props(
                ('app', S.string(description="The slug of the application ``coll`` is in.")),
                ('coll', S.string(description="The slug of the collection to search for documents in.")),
                ('related_key', S.string(description="The name of the key to search for this document in."
                    "If none, defaults to the first matching foreign key element.")),
        )),
        response_schema=S.object(properties=S.props()),
    )
    def rel(self, app, coll, related_key=None):
        """
        Reverse relation.  Get a query set of all documents in a collection that have foreign keys that point to this
            document.

        Args:
            app (str): The slug of the application ``coll`` is in.
            coll (str): The slug of the collection to search for documents in.
            related_key (:obj:`str`, optional): The name of the key to search for this document in.
                If none, defaults to the first matching foreign key element.

        Returns:
            A sondra.collection.QuerySet object

        Raises:
            KeyError: If there are no foreign keys to this document's collection specified on the target's schema. An
                empty result will be just that.  This error is only raised when the foreign-key is not specified.

        """
        c = self.suite[app][coll]
        if not related_key:
            for k, v in c.document_class.specials.items():
                if isinstance(v, ForeignKey) and v.app == self.application.slug and v.coll == self.collection.slug:
                    related_key = k
                    break
            else:
                raise KeyError("Cannot find any foreign keys to {0}/{1}".format(app, coll))

        return c.filter(**{related_key: self.id})


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
        return json.dumps(self.json_repr(), *args, **kwargs)

    def rql_repr(self):
        ret = deepcopy(self.obj)

        value_handlers = self.specials or {}
        for k, handler in value_handlers.items():
            if k in ret:
                ret[k] = handler.to_rql_repr(ret[k], self)

        return ret

    def json_repr(self, ordered=False, bare_keys=False):
        js = deepcopy(self.obj)

        for property, special in self.specials.items():
            if property in js:
                js[property] = special.to_json_repr(js[property], self, bare_keys=bare_keys)

        # if ordered:
        #     js = natural_order(js, self.property_order)

        if self.saved:
            js['_url'] = self._url

        return js

    def geojson_repr(self, ordered=False, bare_keys=False):
        js = self.json_repr(ordered, bare_keys)

        if 'feature_geometry' in self.schema:
            geom = js.get(self.schema['feature_geometry'], None)
        else:
            GEOM_TYPES = {'geometry', 'point','linestring','polygon','multipoint','multilinestring','multipolygon'}
            geoms = filter(lambda v: isinstance(v, dict) and v.get('type', '').lower() in GEOM_TYPES, js.values())
            try:
                geom = next(geoms)
            except StopIteration:
                geom = None

        # if ordered:
        #     js = natural_order(js, self.property_order)

        if self.saved:
            js['_url'] = self._url

        return {
            "type": "Feature",
            "geometry": geom,
            "properties": js
        }

    def save(self, conflict='replace', *args, **kwargs):
        ret = self.collection.save(self, conflict=conflict, *args, **kwargs)
        return ret

    def delete(self, **kwargs):
        ret =  self.collection.delete(self, **kwargs)
        return ret

    def validate(self):
        jsonschema.validate(self.obj, self.schema)

    @deprecated
    def pre_save(self):
        pass

    @deprecated
    def post_save(self):
        pass

    @deprecated
    def pre_delete(self):
        pass

    @deprecated
    def post_delete(self):
        pass