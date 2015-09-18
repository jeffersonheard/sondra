"""Sondra's JSON API Services.

API services are provided as a part of Sondra.  These will get more complete over time.
In the meantime, here is the general combined URL format for calls on a collection::

    http://host/api/appname.appmethod/collection.classmethod/0000.instancemethod/@/frag/ment;format
                    1------ 2-------- 3--------- 4---------- 5--- 6------------- 7---------- 8-----

1. **appname**. The application name, in this case "pronto"
2. **appmethod**. A ``@expose``d method that exists on the application object itself.
3. **collection**. The collection name. If the collection is in subgroups, the subgroups are
   path-separated as well
4. **classmethod**. Any exposed class methods on the
5. **oid**. The object id. This is a UUID-based object, like RethinkDB usually uses.
6. **instancemethod**. A method name that was ``@expose``d on the model.
7. **format**. ``(json|jsonp|html|help|schema)`` The format we expect it in. ``json`` is
   generally the default.
8. **fragment**. The fragment portion of the URL digs deeper into the properties of the object,
   referring to a JSON fragment within the referred document. Numbers and Python slice syntax work
   fine to get at pieces of an array within the document.

Furthermore there's a grammar for combining these, and a list of allowed HTTP methods for each
grammar production

Application Metadata
====================

Form::

    http://localhost:5000/appname/api;format

By format:

* ``help``: Get the docstring for the application object
* ``schema``: Get the combined JSON schemas for all collections for the application

Application Method Call
=======================

Form::

    http://localhost:5000/appname/api.method.slug;format

All Applications have the following methods:

- **listing** - get the listing of all APIs and methods on this application

``GET`` and ``POST`` have the same meaning. All other methods are unsupported on the application.

GET Behavior
------------

By format:

* ``help``: Get the docstring of the method.
* ``schema``: Get the request and response schema in JSON Schema format.
* ``html``:  Get a string response to the method.
* ``json``:  Get a JSON response to the method.
* ``jsonp`: Get a JSON-P response to the method.

GET expects you to call the method with arguments passed as query parameters. For more complicated
method calls, use POST

POST Behavior
-------------

POST always calls the method. POST can either be called with urlencoded or multipart form data, or
with a JSON string. All of these will be interpreted the same way, as method calls.  Only multipart
supports FILE posts, and the files posted must be according to the method parameters.

By format:

Formats ``form, help, schema`` are unsupported.

* ``json``: Expect a JSON response.
* ``jsonp``: Expect a JSON response.
* ``html``: Expect a JSON response.

Listing
-------

**To be documented**

Collection Metadata
===================

Form::

    http://localhost:5000/appname/api/collection;format

By format:

* ``help``: Get the docstring for the collection object
* ``schema``: Get the JSON Schema for this collection.

Class Method Call
=================

Form::

    http://localhost:5000/appname/api/collection.classmethod

All collections have the following methods:

- **listing** - get the listing of all APIs and methods on this class
- **filter** - query the collection underlying the API

Listing
-------

**To be documented**

Filter
------

**To be documented**


Instance Method Call
====================

Form::

    http://localhost:5000/appname/api/collection/uuid.instancemethod;format

Instance methods follow the same pattern as application and collection method calls. There are no
default instance methods.

Object Create, List
===================

Form::

    http://localhsot:5000/appname/api/collection;format

``GET`` Retrieve a listing of objects
--------------------------------------
Retrieve a listing of objects in the given format, or retrieve an HTML form for entering a new
object.

Query parameters accepted:

* **offset**: Retrieve objects, starting with **offset**-th object
* **count**: The number of objects to return.

``POST`` Add/Update an object
-----------------------------

Accepts JSON in the post body. The object being POST-ed is considered to be a wholesale replacement
for the object that already exists.  Objects are looked up by their 'id' field, if present. The
response will the the object that was added, with 'id' field added if it wasn't already present.

Object Detail, Update, Delete
=============================

Form::

    http://localhost:5000/appname/api/collection/oid;format#/fragment

``GET`` Behavior
----------------

Retrieves the object specified by ``oid`` and optionally ``fragment``.  Fragment is a slash-
separated path to a particular attribute on the requested object.

Formats:

` ``html``: Get an HTML fragment rendered by the object's ``__str__(self)`` method.
- ``json``: Get the JSON corresponding to the object
- ``jsonp``: Get the JSON corresopnding to the object as a JSON-P string

``POST`` Behavior
-----------------

Accepts JSON in the request body. The object being POST-ed is treated as a wholesale replacement
for the object that already exists.  Objects are looked up by their 'id' field, if present. The
response will the the object that was added, with 'id' field added if it wasn't already present.

The response is the object (fragment).

Formats:

- ``html``: Get an HTML fragment rendered by the object's ``__str__(self)`` method.
- ``json``: Get the JSON corresponding to the object
- ``jsonp``: Get the JSON corresopnding to the object as a JSON-P string

``PUT`` and ``PATCH`` Behavior
------------------------------

Accepts JSON in the request body.  The object of a PUT or PATCH request body is considered only to
be updates on the object. All original object attributes that aren't specified are kept.  The
assignment is also relative to the ``fragment`` section of the URL, so the replacement / update is
of the object located at that path. If the fragment you PUT or PATCH to refers to an array, the
object in the body is appended to that array. If you wish to replace the array, use a POST request.

The response is the updated object fragment.

Formats:

- ``html``: Get an HTML fragment rendered by the object's ``__str__(self)`` method.
- ``json``: Get the JSON corresponding to the object
- ``jsonp``: Get the JSON corresopnding to the object as a JSON-P string


A Note on Query strings
=======================

The query string can be used to pass arguments to methods defined on the collection that were
decorated with @exposable.

"""
import inspect
import io
import json
import jsonschema
from functools import partial
from urllib.parse import urlencode
import rethinkdb as r

from .ref import Reference
from . import document
from .suite import BASIC_TYPES
from .utils import mapjson

def to_reql_types(doc):
    jsonschema.validate(doc, BASIC_TYPES['datetime'])
    return document.Time().to_rql_repr(doc)

_parse_query = partial(mapjson, to_reql_types)

class ValidationError(Exception):
    pass


class RequestProcessor(object):
    def process_api_request(self, r):
        return r

    def __call__(self, *args, **kwargs):
        self.process_api_request(*args, **kwargs)


class APIRequest(object):
    formats = {'help', 'json', 'schema', 'html', 'js'}
    DEFAULT_FORMAT = 'json'
    MAX_RESULTS = 100
    SAFE_OPS = {
        'with_fields',
        'count',
        'max',
        'min',
        'avg',
        'sample',
        'sum',
        'distinct',
        'contains',
        'pluck',
        'without',
        'has_fields',
        'order_by',
        'between'
    }

    GEOSPATIAL_OPS = {
        'distance',
        'get_intersecting',
        'get_nearest',
    }

    def __init__(self, suite, headers, body, method, user, path, query_params, files):
        self.suite = suite
        self.headers = headers
        self.body = body
        self.request_method = method
        self.user = user
        self.query_params = query_params or {}
        self.files = files
        self.arguments = []
        self.api_arguments = {}

        self.reference = Reference(
            self.suite,
            "{0}?{1}".format(
                 path,
                 urlencode(query_params) if not isinstance(query_params, str) else query_params
            )
        )

        self._get_arguments()


        if self.reference.kind in {'collection','document','subdocument'} and self.reference.get_collection().private:
            raise PermissionError("The collection referenced in this request is marked private. It can only be used locally.")

        self.decision_tree = {
            'help': {
                'application': {
                    'GET': self.help,
                    'POST': self.help,
                },
                'application_method': {
                    'GET': self.help,
                    'POST': self.help,
                },
                'collection': {
                    'GET': self.help,
                    'POST': self.help,
                },
                'collection_method': {
                    'GET': self.help,
                    'POST': self.help,
                },
                'document': {
                    'GET': self.help,
                    'POST': self.help,
                },
                'document_method': {
                    'GET': self.help,
                    'POST': self.help,
                },
                'subdocument': {},
            },
            'json': {
                'application': {},
                'application_method': {
                    'GET': partial(self.json_response, self.application_method),
                    'POST': partial(self.json_response, self.application_method),
                },
                'collection': {
                    'GET': partial(self.json_response, self.get_collection_items),
                    'POST': partial(self.json_response, self.add_collection_items),
                    'PUT': partial(self.json_response, self.update_collection_items),
                    'DELETE': partial(self.json_response, self.delete_collection_items),
                },
                'collection_method': {
                    'GET': partial(self.json_response, self.collection_method),
                    'POST': partial(self.json_response, self.collection_method),
                },
                'document': {
                    'GET': partial(self.json_response, self.get_document),
                    'POST': partial(self.json_response, self.set_document),
                    'PUT': partial(self.json_response, self.update_document),
                    'DELETE': partial(self.json_response, self.delete_document),
                },
                'document_method': {
                    'GET': partial(self.json_response, self.document_method),
                    'POST': partial(self.json_response, self.document_method),
                },
                'subdocument': {
                    'GET': partial(self.json_response, self.get_subdocument),
                    'POST': partial(self.json_response, self.replace_subdocument),
                    'PUT': partial(self.json_response, self.update_or_append_subdocument),
                    'DELETE': partial(self.json_response, self.delete_subdocument),
                }
            },
            'geojson': {
                'application': {},
                'application_method': {
                    'GET': partial(self.geojson_response, self.application_method),
                    'POST': partial(self.geojson_response, self.application_method),
                    },
                'collection': {
                    'GET': partial(self.geojson_response, self.get_collection_items),
                    },
                'collection_method': {
                    'GET': partial(self.geojson_response, self.collection_method),
                    },
                'document': {
                    'GET': partial(self.geojson_response, self.get_document),
                    },
                'document_method': {
                    'GET': partial(self.geojson_response, self.document_method),
                    'POST': partial(self.geojson_response, self.document_method),
                },
                'subdocument': {
                    'GET': partial(self.geojson_response, self.get_subdocument),
                    }
            },
            'jsonp': {
                'application': {},
                'application_method': {
                    'GET': partial(self.jsonp_response, self.application_method),
                    'POST': partial(self.jsonp_response, self.application_method),
                },
                'collection': {
                    'GET': partial(self.jsonp_response, self.get_collection_items),
                    'POST': partial(self.jsonp_response, self.add_collection_items),
                    'PUT': partial(self.jsonp_response, self.update_collection_items),
                    'DELETE': partial(self.jsonp_response, self.delete_collection_items),
                },
                'collection_method': {
                    'GET': partial(self.jsonp_response, self.collection_method),
                    'POST': partial(self.jsonp_response, self.collection_method),
                },
                'document': {
                    'GET': partial(self.jsonp_response, self.get_document),
                    'POST': partial(self.jsonp_response, self.set_document),
                    'PUT': partial(self.jsonp_response, self.update_document),
                    'DELETE': partial(self.jsonp_response, self.delete_document),
                },
                'document_method': {
                    'GET': partial(self.jsonp_response, self.document_method),
                    'POST': partial(self.jsonp_response, self.document_method),
                },
                'subdocument': {
                    'GET': partial(self.jsonp_response, self.get_subdocument),
                    'POST': partial(self.jsonp_response, self.replace_subdocument),
                    'PUT': partial(self.jsonp_response, self.update_or_append_subdocument),
                    'DELETE': partial(self.jsonp_response, self.delete_subdocument),
                }
            },
            'schema': {
                'application': {
                    'GET': partial(self.json_response, self.application_schema),
                    'POST': partial(self.json_response, self.application_schema),
                },
                'application_method': {
                    'GET': partial(self.json_response, self.application_method_schema),
                    'POST': partial(self.json_response, self.application_method_schema),
                },
                'collection': {
                    'GET': partial(self.json_response, self.collection_schema),
                    'POST': partial(self.json_response, self.collection_schema),
                },
                'collection_method': {
                    'GET': partial(self.json_response, self.collection_method_schema),
                    'POST': partial(self.json_response, self.collection_method_schema),
                },
                'document': {
                    'GET': partial(self.json_response, self.document_schema),
                    'POST': partial(self.json_response, self.document_schema),
                },
                'document_method': {
                    'GET': partial(self.json_response, self.document_method_schema),
                    'POST': partial(self.json_response, self.document_method_schema),
                }
            },
            'html': {
                'application': {},
                'application_method': {
                    'GET': partial(self.html_response, self.application_method),
                    'POST': partial(self.html_response, self.application_method),
                },
                'collection': {
                    'GET': partial(self.html_response, self.get_collection_items),
                    'POST': partial(self.html_response, self.add_collection_items),
                    'PUT': partial(self.html_response, self.update_collection_items),
                    'DELETE': partial(self.html_response, self.delete_collection_items),
                },
                'collection_method': {
                    'GET': partial(self.html_response, self.collection_method),
                    'POST': partial(self.html_response, self.collection_method),
                },
                'document': {
                    'GET': partial(self.html_response, self.get_document),
                    'POST': partial(self.html_response, self.set_document),
                    'PUT': partial(self.html_response, self.update_document),
                    'DELETE': partial(self.html_response, self.delete_document),
                },
                'document_method': {
                    'GET': partial(self.html_response, self.document_method),
                    'POST': partial(self.html_response, self.document_method),
                },
                'subdocument': {
                    'GET': partial(self.html_response, self.get_subdocument),
                    'POST': partial(self.html_response, self.replace_subdocument),
                    'PUT': partial(self.html_response, self.update_or_append_subdocument),
                    'DELETE': partial(self.html_response, self.delete_subdocument),
                }
            },
        }

    def __call__(self):
        kind = self.reference.kind
        method = self.request_method
        format = self.reference.format

        return self.decision_tree[format][kind].get(method, self.exceptional_request)()


    def _get_arguments(self):
        """Turn body and query string into a JSON-serializable data structure"""

        target = self.reference.value

        if self.reference.kind.endswith('method'):
            schema = target.request_schema(target)
        elif self.reference.kind in ['collection', 'application']:
            schema = target.schema
        else:
            schema = target.collection.schema

        # user must pass real object(s) as URL-encoded JSON in the 'q' parameter
        if 'q' in self.query_params:
            v = json.loads(self.query_params['q'][0])
            if isinstance(v, dict):
                self.arguments.append(v)
            else:
                self.arguments.extend(v)
            del self.query_params['q']

        # Validate and copy query params into arguments dictionary
        if self.query_params:
            for k, v in self.query_params.items():
                v = v[0]  # no list valued arguments in api-args
                if v.startswith('"'):
                    v = v[1:-1]
                else:
                    try:
                        v = int(v)
                    except ValueError:
                        try:
                            v = float(v)
                        except ValueError:
                            pass
                self.api_arguments[k] = v

        if self.body:
            if isinstance(self.body, bytes):
                body_args = json.loads(self.body.decode('utf-8'))
            elif isinstance(self.body, str):
                body_args = json.loads(self.body)
            else:
                body_args = self.body

            if isinstance(body_args, list):
                self.arguments.extend(body_args)
            else:
                self.arguments.append(body_args)

        if self.files:
            self.arguments[0].update(self.files)

        self.durability = self.api_arguments.get('durability', 'hard')
        self.return_changes = self.api_arguments.get('return_changes', 'false').lower() != 'false'
        self.dereference = self.api_arguments.get('dereference', 'false').lower() != 'false'
        self.delete_all = self.api_arguments.get('delete_all', 'false').lower() != 'false'
        self.conflict = self.api_arguments.get('conflict', {
            'POST': "error",
            'PUT': "replace",
            'PATCH': "replace"
        }.get(self.request_method, 'error'))

        for a in self.arguments:
            jsonschema.validate(a, schema)


    def help(self):
        """Return HTML help from a docstring"""
        # FIXME this should check read/execute permissions on all objects

        kind = self.reference.kind

        out = io.StringIO()

        # create a title from the string
        def rst_title(name):
            out.write("\n{fmt}\n{name}\n{fmt}\n\n".format(
                fmt="#" * len(name),
                name=name
            ))

        # create a section header (up to 5 levels) from the string
        def rst_section(name, level=0):
            out.write("\n{name}\n{fmt}\n\n".format(
                fmt=["#", ">", "=", "-", "_"][level]*len(name),
                name=name
            ))

        # format literal code
        def code(lines, amount=4, ch=' '):
            padding = amount * ch
            out.write(padding + ('\n'+padding).join(lines.split('\n')))
            out.write("\n\n")

        # trawl through the object, finding exposable methods and documenting them
        def method_docs(obj, level=1):
            for name, method in inspect.getmembers(obj, predicate=inspect.ismethod):
                if hasattr(method, 'exposed'):  # then this is an exposable method
                    rst_section(method.slug, level=level)
                    out.write("**Link**: ``{0}.{1}``\n\n".format(obj.url, method.slug))
                    out.write("**Argument schema**::\n\n")
                    code(json.dumps(method.request_schema(method), indent=4))
                    out.write("**Return schema**::\n\n")
                    code(json.dumps(method.response_schema(method), indent=4))
                    out.write("**Documentation**::\n\n")
                    out.write(method.__doc__ or "*No documentation provided.*\n")

        # document the application, all its methods, then all its collections and their methods
        if kind == 'application':
            obj = self.reference.get_application()

            rst_title(obj.name)
            out.write("**Link**: {0}\n\n".format(obj.url))
            out.write(obj.__doc__ or "*No documentation provided.*\n")
            out.write("\n")
            rst_section('Methods')
            method_docs(obj)

            rst_section('Collections')
            for coll in obj.values():
                rst_section(coll.name, 1)
                out.write("**Link**: {0}\n\n".format(coll.url))
                out.write(coll.__doc__ or "*No documentation provided.*\n")
                out.write("\n")
                out.write("**Schema**::\n\n")
                code(json.dumps(coll.schema, indent=4))
                rst_section("Collection Methods", 1)
                method_docs(coll, 2)
                rst_section("Document Methods", 1)
                method_docs(coll.document_class, 2)

        # document the collection's schema, all its methods, then its documents' methods
        elif kind == 'collection':
            obj = self.reference.get_collection()
            rst_title(obj.name)
            out.write("**Application docs**: App_")
            out.write("**Link**: {0}\n\n".format(obj.url))
            out.write(obj.__doc__ or "*No documentation provided.*\n")
            out.write("\n")
            out.write("**Original Schema**::\n\n")
            code(json.dumps(obj.original_schema))
            out.write("**API Schema**::\n\n")
            code(json.dumps(obj.schema, indent=4))
            rst_section("Collection Methods", 1)
            method_docs(obj, 2)
            rst_section("Document Methods", 1)
            method_docs(obj.document_class, 2)
            out.write("\n\n.. App_: {0}".format(obj.application.url))

        # document this document's schema collection methods and its document methods
        elif kind == 'document':
            obj = self.reference.get_document()
            rst_title(obj.collection.name + ": " + obj.id)
            out.write("**Application docs**: App_")
            out.write("**Link**: {0}\n\n".format(obj.url))
            out.write(obj.collection.__doc__ or "*No documentation provided.*\n")
            out.write("\n")
            out.write("**Schema**::\n\n")
            code(json.dumps(obj.collection.schema, indent=4))
            out.write("**Value**::\n\n")
            code(obj.json(indent=4))
            rst_section("Collection Methods", 1)
            method_docs(obj.collection, 2)
            rst_section("Document Methods", 1)
            method_docs(obj, 2)
            out.write("\n\n.. App_: {0}".format(obj.application.url))

        # document the method and its schema
        elif kind == 'application_method':
            method = self.reference.get_application_method()
            rst_title(method.__self__.slug + "." + method.slug)
            out.write("**Application docs**: App_")
            out.write("**Link**: {0}.{1}\n\n".format(method.__self__.url, method.slug))
            out.write("**Request schema**::\n\n")
            code(json.dumps(method.request_schema(method), indent=4))
            out.write("**Response schema**::\n\n")
            code(json.dumps(method.response_schema(method), indent=4))
            out.write("**Documentation**\n\n")
            out.write(method.__doc__ or "*No documentation provided.*\n")
            out.write("\n\n.. App_: {0}".format(method.__self__.application.url))

        # document the method and its schema
        elif kind == 'collection_method':
            method = self.reference.get_collection_method()
            rst_title(method.__self__.slug + "." + method.slug)
            out.write("**Application docs**: App_")
            out.write("**Link**: {0}.{1}\n\n".format(method.__self__.url, method.slug))
            out.write("**Argument schema**::\n\n")
            code(json.dumps(method.request_schema(method), indent=4))
            out.write("**Return schema**::\n\n")
            code(json.dumps(method.response_schema(method), indent=4))
            out.write("**Documentation**\n\n")
            out.write(method.__doc__ or "*No documentation provided.*\n")
            out.write("\n\n.. App_: {0}".format(method.__self__.application.url))

        # document the method and its schema
        elif kind == 'document_method':
            method = self.reference.get_document_method()
            rst_title(method.__self__.id + "." + method.slug)
            out.write("**Application docs**: App_")
            out.write("**Link**: {0}.{1}\n\n".format(method.__self__.name.url, method.slug))
            out.write("**Argument schema**::\n\n")
            code(json.dumps(method.request_schema(method), indent=4))
            out.write("**Return schema**::\n\n")
            code(json.dumps(method.response_schema(method), indent=4))
            out.write("**Documentation**\n\n")
            out.write(method.__doc__ or "*No documentation provided.*\n")
            out.write("\n\n.. App_: {0}".format(method.__self__.application.url))

        return 'text/html', self.suite.docstring_processor(out.getvalue())

    def json_response(self, method):
        result = method()
        def fun(doc):
            if isinstance(doc, document.Document):
                return mapjson(fun, doc.obj)
            else:
                return doc

        result = mapjson(fun, result)  # make sure to serialize a full Document structure if we have one.

        return 'application/json', json.dumps(result, indent=4)

    def geojson_response(self, method):
        result = method()
        def fun(doc):
            if isinstance(doc, document.Document):
                if doc.collection and doc.collection.specials:
                    for s, t in doc.collection.specials.items():
                        if isinstance(t, document.Geometry):
                            result = mapjson(fun, doc.obj)
                            result = {
                                "type": "Feature",
                                "geometry": doc[s],
                                "properties": result
                            }
                            break
                    else:
                        result = mapjson(fun, doc.obj)
                    return result
            else:
                return doc

        result = mapjson(fun, result)  # make sure to serialize a full Document structure if we have one.

        if isinstance(result, list):
            ret = {
                "type": "FeatureCollection",
                "features": result
            }
        else:
            ret = result
        return 'application/json', json.dumps(ret, indent=4)

    def jsonp_response(self, method):
        raise NotImplementedError()

    def html_response(self, method):
        raise NotImplementedError()

    def application_method(self):
        method = self.reference.get_application_method()
        if len(self.arguments):
            ret = method(**self.arguments[0])
        else:
            ret = method()
        return ret

    def collection_method(self):
        method = self.reference.get_collection_method()
        if len(self.arguments):
            ret = method(**self.arguments[0])
        else:
            ret = method()
        return ret

    def document_method(self):
        method = self.reference.get_document_method()
        if len(self.arguments):
            ret = method(**self.arguments[0])
        else:
            ret = method()
        return ret

    def _handle_simple_filters(self, q):
        # handle simple filters
        if 'flt' in self.api_arguments:
            flt = json.loads(self.api_arguments['flt'])
            if isinstance(flt, dict):
                flt = [flt]

            flt = _parse_query(flt)
            for f in flt:
                default = flt.get('default', False)
                op = f.get('op', '==')
                if op == '==':
                    if 'op' not in f:
                        q = q.filter(f, default=default)
                    else:
                        q = q.filter(f['args'], default=default)
                elif op == '!=':
                    q = q.filter(f['lhs'] != f['rhs'], default=default)
                elif op == '<':
                    q = q.filter(f['lhs'] < f['rhs'], default=default)
                elif op == '<=':
                    q = q.filter(f['lhs'] <= f['rhs'], default=default)
                elif op == '>':
                    q = q.filter(f['lhs'] > f['rhs'], default=default)
                elif op == '>=':
                    q = q.filter(f['lhs'] >= f['rhs'], default=default)
                elif op == 'match':
                    field = f['lhs']
                    pattern  = f['rhs']
                    q = q.filter(lambda x: x[field].match(pattern), default=default)
                elif op == 'contains':
                    field = f['lhs']
                    pattern  = f['rhs']
                    q = q.filter(lambda x: x[field].contains(pattern))
                elif op == 'has_fields':
                    q = q.filter(lambda x: x.has_fields(f['fields']), default=default)
                else:
                    raise ValidationError("Unrecognized op in filter specification.")
        return q

    def _handle_spatial_filters(self, coll, q):
        # handle geospatial queries
        if 'geo' in self.api_arguments:  # unindent is intentional. Allow spatial queries limiting grouped pk lookups
            geo = json.loads(self.api_arguments['geo'])
            if not coll.geometries:
                raise ValidationError("Requested a geometric query on a non geometric collection")
            if 'against' not in geo:
                test_property = next(coll.geometries.keys())
            else:
                test_property = geo['against']
            op = geo['op']
            geom = r.geojson(geo['test'])
            if op['name'] not in self.GEOSPATIAL_OPS:
                raise ValidationError("Cannot perform non geometry op in geometry query")
            q = getattr(q, op['name'])(geom, index=test_property, *op.get('args',[]), **op.get('kwargs', {}))
        return q

    def _handle_aggregations(self, q):
        # handle aggregation queries
        if 'agg' in self.api_arguments:
            op = json.loads(self.api_arguments['agg'])
            if op['name'] not in self.SAFE_OPS:
                raise ValidationError("Cannot perform unsafe op in GET")
            q = getattr(q, op['name'])(*op.get('args',[]), **op.get('kwargs', {}))
        return q

    def _handle_limits(self, q):
        # handle start, limit, and end
        if 'start' and 'end' in self.api_arguments:
            s = self.api_arguments['start']
            e = self.api_arguments['end']
            if e == 0:
                q = q.skip(s)
            else:
                q = q.slice(s, e)
        else:
            if 'start' in self.api_arguments:
                s = self.api_arguments['start']
                q = q.skip(s)
            if 'limit' in self.api_arguments:
                limit = self.api_arguments['limit']
                q = q.limit(limit)
            else:
                q = q.limit(self.MAX_RESULTS)
        return q

    def get_collection_items(self):
        coll = self.reference.get_collection()

        if len(self.arguments):
            if 'index' in self.api_arguments:
                q = coll.table.get_all(self.arguments, index=self.api_arguments['index'])
            else:
                q = coll.table.get_all(self.arguments)
        else:
            q = coll.table

        q = self._handle_simple_filters(q)
        q = self._handle_spatial_filters(coll, q)
        q = self._handle_aggregations(q)
        q = self._handle_limits(q)

        if self.dereference:
            return [x.dereference() for x in coll.q(q)]
        else:
            return [x for x in coll.q(q)]

    def add_collection_items(self):
        coll = self.reference.get_collection()
        changes = coll.save(self.arguments, conflict='error', return_changes=True)
        keys = [(coll.url + '/' + k) for k in (value['new_val'][coll.primary_key] for value in changes['changes'])]
        return keys

    def update_collection_items(self):
        coll = self.reference.get_collection()
        docs = []
        for k, updates in self.arguments:
            doc = coll[k]
            for prop, value in updates.items():
                doc[prop] = value

            docs.append(doc)

        ret = coll.save(docs, conflict=self.conflict, durability=self.durability, return_changes=self.return_changes)
        return ret

    def replace_collection_items(self):
        coll = self.reference.get_collection()
        docs = []
        for k, new_value in self.arguments:
            doc = coll[k].doc(new_value)
            doc.id = k
            docs.append(doc)

        ret = coll.save(docs, conflict=self.conflict, durability=self.durability, return_changes=self.return_changes)
        return ret

    def delete_collection_items(self):
        coll = self.reference.get_collection()

        if len(self.arguments):
            if 'index' in self.api_arguments:
                q = coll.table.get_all(self.arguments, index=self.api_arguments['index'])
            else:
                q = coll.table.get_all(self.arguments)
        elif self.delete_all:
            q = coll.table
        else:
            raise PermissionError("Cannot delete all collection items without a specific request.")

        q = self._handle_simple_filters(q)
        q = self._handle_spatial_filters(coll, q)

        return q.delete(durability=self.durability, return_changes=self.return_changes).run(coll.application.connection)

    def get_document(self):
        doc = self.reference.get_document()
        if self.dereference:
            doc.collection.application.dereference(doc)
        return doc

    def set_document(self):
        doc = self.reference.get_document()
        id = doc.id
        new_doc = doc.collection.doc(self.arguments)
        new_doc.id = id
        ret = new_doc.save(conflict=self.conflict, durability=self.durability, return_changes=self.return_changes)
        return ret

    def update_document(self):
        doc = self.reference.get_document()
        if doc.collection.primary_key in self.arguments[0]:
            del self.arguments[0][doc.collection.primary_key]
        for k, v in self.arguments[0].items():
            doc[k] = v
        ret = doc.save(conflict='replace', durability=self.durability, return_changes=self.return_changes)
        return ret

    def delete_document(self):
        doc = self.reference.get_document()
        return doc.delete(durability=self.durability, return_changes=self.return_changes)

    def get_subdocument(self):
        doc, parent, walk, frag = self.reference.get_subdocument()
        if isinstance(frag, document.Document):
            frag.reference()
        elif isinstance(frag, list) or isinstance(frag, dict):
            frag = document.references(frag)
        else:
            frag = {"_": frag}

        return frag

    def replace_subdocument(self):
        doc, parent, walk, frag = self.reference.get_subdocument()
        args = self.arguments

        target = parent
        key, *walk = walk
        while walk:
            target = target[key]
            key, *walk = walk
        target[key] = args
        target.save()
        doc = doc.collection[doc.id]  # fetch again now that we've updated
        if self.dereference:
            doc.dereference()
        return doc

    def update_or_append_subdocument(self):
        doc, parent, walk, frag = self.reference.get_subdocument()
        args = self.arguments

        target = parent
        key, *walk = walk
        while walk:
            target = target[key]
            key, *walk = walk
        if isinstance(target[key], dict):
            target[key].update(args[0])
        elif isinstance(target[key], list):
            target[key].append(args)
        elif isinstance(target[key], document.Document):
            for k, v in args[0].items():
                target[key][k] = v
            target[key].save()
            target = target.collection[doc.id]  # fetch again since we updated the target
        target.save()
        doc = doc.collection[doc.id]  # fetch again now that we've updated
        if self.dereference:
            doc.dereference()
        return doc

    def delete_subdocument(self):
        doc, parent, walk, frag = self.reference.get_subdocument()

        target = parent
        key, *walk = walk
        while walk:
            target = target[key]
            key, *walk = walk
        if isinstance(target[key], dict):
            del target[key]
        elif isinstance(target[key], list):
            del target[key]
        elif isinstance(target[key], document.Document):
            target[key].delete()
            del target[key]
        target.save()
        doc = doc.collection[doc.id]  # fetch again now that we've updated
        if self.dereference:
            doc.dereference()
        return doc

    def application_schema(self):
        return self.reference.get_application().schema

    def application_method_schema(self):
        method = self.reference.get_application_method()
        return method.schema(method)

    def collection_schema(self):
        return self.reference.get_collection().schema

    def collection_method_schema(self):
        method = self.reference.get_collection_method()
        return method.schema(method)

    def document_schema(self):
        return self.reference.get_document().schema

    def document_method_schema(self):
        method = self.reference.get_document_method()
        return method.schema(method)

    def exceptional_request(self):
        raise ValidationError(self.reference.url)