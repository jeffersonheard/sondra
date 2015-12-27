"""Sondra's JSON API Services."""
import json
import jsonschema
from functools import partial
from urllib.parse import urlencode
import rethinkdb as r

from sondra.document import Geometry
from sondra.expose import method_schema, method_help
from .ref import Reference
from . import document
import sondra.collection
from .suite import BASIC_TYPES
from .utils import mapjson

class ValidationError(Exception):
    pass


class RequestProcessor(object):
    def process_api_request(self, r):
        return r

    def __call__(self, *args, **kwargs):
        return self.process_api_request(*args, **kwargs)


class APIRequest(object):
    formats = {'help', 'json', 'schema', 'geojson'}
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

    def __str__(self):
        return """
{request_method} {url}

Headers
-------
{headers}

API Arguments
-------------
{api_args}

Objects
-------------
{objects}
        """.format(
            request_method=self.request_method,
            url=self.reference,
            headers="\n".join(['{0}: {1}'.format(*i) for i in self.headers.items()]) if self.headers else "<none>",
            api_args="\n".join(['{0}: {1}'.format(*i) for i in self.api_arguments.items()]) if self.api_arguments else "<none>",
            objects="\n".join(['{0}: {1}'.format(*i) for i in enumerate(self.objects)]) if self.objects else "<none>"
        )

    def __init__(self, suite, headers, body, method, user, path, query_params, files):
        self.suite = suite
        self.headers = headers
        self.body = body
        self.request_method = method.upper()
        self.user = user
        self.query_params = query_params or {}
        self.files = files
        self.objects = []
        self.api_arguments = {}
        self.query = None

        self.reference = Reference(
            self.suite,
            "{0}?{1}".format(
                 path,
                 urlencode(query_params) if not isinstance(query_params, str) else query_params
            )
        )

        self._parse_query()

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
                    'GET': partial(self.json_response, self.method_call),
                    'POST': partial(self.json_response, self.method_call),
                },
                'collection': {
                    'GET': partial(self.json_response, self.get_collection_items),
                    'POST': partial(self.json_response, self.add_collection_items),
                    'PUT': partial(self.json_response, self.replace_collection_items),
                    'PATCH': partial(self.json_response, self.update_collection_items),
                    'DELETE': partial(self.json_response, self.delete_collection_items),
                },
                'collection_method': {
                    'GET': partial(self.json_response, self.method_call),
                    'POST': partial(self.json_response, self.method_call),
                },
                'document': {
                    'GET': partial(self.json_response, self.get_document),
                    'POST': partial(self.json_response, self.set_document),
                    'PUT': partial(self.json_response, self.set_document),
                    'PATCH': partial(self.json_response, self.update_document),
                    'DELETE': partial(self.json_response, self.delete_document),
                },
                'document_method': {
                    'GET': partial(self.json_response, self.method_call),
                    'POST': partial(self.json_response, self.method_call),
                },
                'subdocument': {}
            },
            'geojson': {
                'application': {},
                'collection': {
                    'GET': partial(self.geojson_response, self.get_collection_items),
                    },
                'document': {
                    'GET': partial(self.geojson_response, self.get_document),
                    },

            },
            'schema': {
                'application': {
                    'GET': partial(self.json_response, self.schema),
                    'POST': partial(self.json_response, self.schema),
                },
                'application_method': {
                    'GET': partial(self.json_response, self.schema),
                    'POST': partial(self.json_response, self.schema),
                },
                'collection': {
                    'GET': partial(self.json_response, self.schema),
                    'POST': partial(self.json_response, self.schema),
                },
                'collection_method': {
                    'GET': partial(self.json_response, self.schema),
                    'POST': partial(self.json_response, self.schema),
                },
                'document': {
                    'GET': partial(self.json_response, self.schema),
                    'POST': partial(self.json_response, self.schema),
                },
                'document_method': {
                    'GET': partial(self.json_response, self.schema),
                    'POST': partial(self.json_response, self.schema),
                }
            },
        }

    def __call__(self):
        kind = self.reference.kind
        method = self.request_method
        format = self.reference.format

        return self.decision_tree[format][kind].get(method, self.exceptional_request)()

    def _parse_query(self):
        target = self.reference.value

        if self.reference.kind.endswith('method'):
            schema = method_schema(*target)['definitions']['method_request']
        elif self.reference.kind in ['collection', 'application']:
            schema = target.schema
        else:
            schema = target.collection.schema

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
                self.objects.extend(body_args)
            else:
                if "__q" in body_args:
                    self.api_arguments.update(body_args['__q'])
                    self.request_method = body_args.get('__method', self.request_method).upper()
                    self.objects = body_args.get("__objs", [])
                else:
                    self.objects = [body_args]

        for object in self.objects:
            jsonschema.validate(object, schema)

        if self.files:
            self.objects[0].update(self.files)


        self.durability = self.api_arguments.get('durability', 'hard')
        self.return_changes = self.api_arguments.get('return_changes', 'false').lower() != 'false'
        self.dereference = self.api_arguments.get('dereference', 'false').lower() != 'false'
        self.delete_all = self.api_arguments.get('delete_all', 'false').lower() != 'false'
        self.conflict = self.api_arguments.get('conflict', {
            'POST': "error",
            'PUT': "replace",
            'PATCH': "replace"
        }.get(self.request_method, 'error'))

    def help(self):
        """Return HTML help from a requested object (application, collection, document, method)"""
        # FIXME this should check read/execute permissions on all objects
        if 'method' in self.reference.kind:
            instance, method = self.reference.value
            return 'text/html', self.suite.docstring_processor(method_help(instance, method))
        else:
            value = self.reference.value
            return 'text/html', self.suite.docstring_processor(value.help())

    def json_response(self, method):
        result = method()

        def fun(doc):
            if isinstance(doc, document.Document):
                try:
                    return mapjson(fun, doc.collection.json(doc))
                except Exception as x:
                    print(doc.obj)
                    raise x
            else:
                return doc

        result = mapjson(fun, result)  # make sure to serialize a full Document structure if we have one.

        if not (isinstance(result, dict) or isinstance(result, list)):
            result = {"_": result}

        return 'application/json', json.dumps(result, indent=4)

    def geojson_response(self, method):
        result = method()

        def fun(doc):
            if isinstance(doc, document.Document):
                if doc.specials:
                    for s, t in doc.specials.items():
                        if isinstance(t, sondra.document.Geometry):
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

    def method_call(self):
        instance, method = self.reference.value
        if len(self.objects) > 1:
            ret = [method(**o) for o in self.objects]
        elif len(self.objects) == 1:
            ret = method(**self.objects[0])
        else:
            ret = method()
        return ret

    def _handle_simple_filters(self, q):
        # handle simple filters
        if 'flt' in self.api_arguments:
            flt = json.loads(
                self.api_arguments['flt']) \
                    if isinstance(self.api_arguments['flt'], str) \
                    else self.api_arguments['flt']
            if isinstance(flt, dict):
                flt = [flt]

            print(flt)

            for f in flt:
                default = f.get('default', False)
                op = f.get('op', '==')
                if op == '==':
                    q = q.filter({f['lhs']: f['rhs']}, default=default)
                elif op == '!=':
                    q = q.filter(r.row[f['lhs']] != f['rhs'], default=default)
                elif op == '<':
                    q = q.filter(r.row[f['lhs']] < f['rhs'], default=default)
                elif op == '<=':
                    q = q.filter(r.row[f['lhs']] <= f['rhs'], default=default)
                elif op == '>':
                    q = q.filter(r.row[f['lhs']] > f['rhs'], default=default)
                elif op == '>=':
                    q = q.filter(r.row[f['lhs']] >= f['rhs'], default=default)
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
        if 'geo' in self.api_arguments:
            print("Geospatial limit")
            geo = json.loads(
                self.api_arguments['geo']) \
                    if isinstance(self.api_arguments['geo'], str) \
                    else self.api_arguments['geo']

            geometries = [k for k in coll.document_class.specials if coll.document_class.specials[k].is_geometry]

            if not geometries:
                raise ValidationError("Requested a geometric query on a non geometric collection")
            if 'against' not in geo:
                test_property = geometries[0]
            elif geo['against'] not in geometries:
                raise KeyError('Not a valid geometry name')
            else:
                test_property = geo['against']
            op = geo['op']
            geom = r.geojson(geo['test'])
            if op not in self.GEOSPATIAL_OPS:
                raise ValidationError("Cannot perform non geometry op in geometry query")
            q = getattr(q, op)(geom, index=test_property, *geo.get('args',[]), **geo.get('kwargs', {}))
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

        if len(self.objects):
            if 'index' in self.api_arguments:
                q = coll.table.get_all(self.objects, index=self.api_arguments['index'])
            else:
                q = coll.table.get_all(self.objects)
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
        changes = coll.create(self.objects)
        keys = [v.id for v in changes]
        return keys

    def update_collection_items(self):
        coll = self.reference.get_collection()
        docs = []
        for k, updates in self.objects:
            doc = coll[k]
            for prop, value in updates.items():
                doc[prop] = value

            docs.append(doc)

        ret = coll.save(docs, conflict=self.conflict, durability=self.durability, return_changes=self.return_changes)
        return ret

    def replace_collection_items(self):
        coll = self.reference.get_collection()
        docs = []
        for k, new_value in self.objects:
            doc = coll[k].doc(new_value)
            doc.id = k
            docs.append(doc)

        ret = coll.save(docs, conflict=self.conflict, durability=self.durability, return_changes=self.return_changes)
        return ret

    def delete_collection_items(self):
        coll = self.reference.get_collection()

        if len(self.objects):
            if 'index' in self.api_arguments:
                q = coll.table.get_all(self.objects, index=self.api_arguments['index'])
            else:
                q = coll.table.get_all(self.objects)
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
        new_doc = doc.collection.doc(self.objects[0])
        new_doc.id = id
        ret = new_doc.save(conflict=self.conflict, durability=self.durability, return_changes=self.return_changes)
        return ret

    def update_document(self):
        doc = self.reference.get_document()
        if doc.collection.primary_key in self.objects[0]:
            del self.objects[0][doc.collection.primary_key]
        for k, v in self.objects[0].items():
            doc[k] = v
        ret = doc.save(conflict='replace', durability=self.durability, return_changes=self.return_changes)
        return ret

    def delete_document(self):
        doc = self.reference.get_document()
        return doc.delete(durability=self.durability, return_changes=self.return_changes)

    def schema(self):
        return self.reference.schema

    def exceptional_request(self):
        raise ValidationError("{method} {kind}: {url}".format(method=self.request_method, kind=self.reference.kind, url=self.reference.url))