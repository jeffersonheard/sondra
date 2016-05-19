"""Sondra's JSON API Services."""
import json
import jsonschema
from urllib.parse import urlencode
from textwrap import dedent

from sondra import formatters
from sondra.exceptions import ValidationError
from sondra.expose import method_schema
from sondra.ref import Reference
from sondra.query_set import QuerySet


class RequestProcessor(object):
    """
    Request Processors are arbitrary thunks run before the request is executed. For an example, see the auth application.
    """
    def process_api_request(self, r):
        return r

    def cleanup_after_exception(self, r, e):
        pass

    def __call__(self, r):
        return self.process_api_request(r)


class APIRequest(object):
    """
    Represents and executes a single API request that has been received by the framework.
    """
    formats = {
        'help': formatters.Help(),
        'json': formatters.JSON(),
        'schema': formatters.Schema(),
        'geojson': formatters.GeoJSON()
    }
    DEFAULT_FORMAT = 'json'

    def __str__(self):
        return dedent("""\
            {request_method} {url}

            Headers
            -------
            {headers}

            API Arguments
            -------------
            {api_args}

            Objects
            -------------
            {objects}\
        """).format(
            request_method=self.request_method,
            url=self.reference,
            headers="\n".join(['{0}: {1}'.format(*i) for i in self.headers.items()]) if self.headers else "<none>",
            api_args="\n".join(['{0}: {1}'.format(*i) for i in self.api_arguments.items()]) if self.api_arguments else "<none>",
            objects="\n".join(['{0}: {1}'.format(*i) for i in enumerate(self.objects)]) if self.objects else "<none>"
        )

    def __init__(self, suite, headers, body, method, path, query_params, files):
        self.suite = suite
        self.headers = headers
        self.body = body
        self.request_method = method.upper()
        self.user = None
        self.query_params = query_params or {}
        self.files = files
        self.objects = []
        self.api_arguments = {}
        self.formatter = self.formats[self.DEFAULT_FORMAT]
        self.formatter_kwargs = {}
        self.query = None
        self.additional_filters = []

        self.reference = Reference(
            self.suite,
            "{0}?{1}".format(
                 path,
                 urlencode(query_params) if not isinstance(query_params, str) else query_params
            )
        )

        if self.reference.kind in {'collection','document','subdocument'} and self.reference.get_collection().private:
            raise PermissionError("The collection referenced in this request is marked private. It can only be used locally.")

        self._parse_query()


    def __call__(self):
        kind = self.reference.kind
        method = self.request_method
        format = self.reference.format
        decision_tree = {
            'application_method': {
                'GET': self.method_call,
                'POST': self.method_call,
            },
            'collection': {
                'GET': self.get_collection_items,
                'POST': self.add_collection_items,
                'PUT': self.replace_collection_items,
                'PATCH': self.update_collection_items,
                'DELETE': self.delete_collection_items,
            },
            'collection_method': {
                'GET': self.method_call,
                'POST': self.method_call,
            },
            'document': {
                'GET': self.get_document,
                'POST': self.set_document,
                'PUT': self.set_document,
                'PATCH': self.update_document,
                'DELETE': self.delete_document,
            },
            'document_method': {
                'GET': self.method_call,
                'POST': self.method_call,
            },
        }

        if kind in decision_tree:
            action = decision_tree[kind][method]
            return self.formats[format](self.reference, action(), **self.formatter_kwargs)
        else:
            return self.formats[format](self.reference, self.reference.value, **self.formatter_kwargs)


    def _parse_query(self):
        self.formatter_kwargs = self.reference.kwargs
        self.objects = []

        if self.query_params:
            if '__objs' in self.query_params:
                self.objects = json.loads(self.query_params['__objs'])
                if isinstance(self.objects, dict):
                    self.objects = [self.objects]

            for k, v in self.query_params.items():
                if k.startswith('__'):
                    continue

                if isinstance(v, list):
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
                elif isinstance(body_args, dict):
                    self.objects.append(body_args)
                else:
                    self.objects.extend(body_args)

        self.objects = [{k: v for k, v in obj.items() if v is not None} for obj in self.objects]

        self.durability = self.api_arguments.get('durability', 'hard')
        self.return_changes = self.api_arguments.get('return_changes', 'false').lower() != 'false'
        self.dereference = self.api_arguments.get('dereference', 'false').lower() != 'false'
        self.delete_all = self.api_arguments.get('delete_all', 'false').lower() != 'false'
        self.conflict = self.api_arguments.get('conflict', {
            'POST': "error",
            'PUT': "replace",
            'PATCH': "replace"
        }.get(self.request_method, 'error'))

    def validate(self):
        target = self.reference.value

        if self.reference.kind.endswith('method'):
            schema = method_schema(*target)['definitions']['method_request']
        elif self.reference.kind in ['collection', 'application']:
            schema = target.schema
        else:
            schema = target.collection.schema

        for object in self.objects:
            if not isinstance(object, str):
                jsonschema.validate(object, schema)

    def method_call(self):
        instance, method = self.reference.value
        execute = self.reference.format not in { 'help', 'schema' }  # fixme buggy hardcoded crap.

        if not execute:
            ret = method
        elif (hasattr(method, 'authentication_required') or hasattr(method, 'authorization_required')) and method.authentication_required:
            if len(self.objects) > 1:
                ret = [method(_user=self.user, **o) for o in self.objects]
            elif len(self.objects) == 1:
                ret = method(_user=self.user, **self.objects[0])
            else:
                ret = method(_user=self.user)
        else:
            if len(self.objects) > 1:
                ret = [method(**o) for o in self.objects]
            elif len(self.objects) == 1:
                ret = method(**self.objects[0])
            else:
                ret = method()
        return ret

    def get_collection_items(self):
        coll = self.reference.get_collection()
        if self.reference.format in {'schema', 'help'}:
            return coll

        qs = QuerySet(coll)
        q = qs.get_query(self.api_arguments, self.objects)
        for f in self.additional_filters:
            q = q.filter(f)

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
        for new_value in self.objects:
            assert coll.primary_key in new_value
            doc = coll.doc(new_value)
            docs.append(doc)

        ret = coll.save(docs, conflict=self.conflict, durability=self.durability, return_changes=self.return_changes)
        return ret

    def delete_collection_items(self):
        coll = self.reference.get_collection()

        qs = QuerySet(coll)
        q = qs.get_query(self.api_arguments, self.objects)
        for f in self.additional_filters:
            q = q.filter(f)

        print("Delete collection items")
        print(q)
        if not self.delete_all and not qs.is_restricted(self.api_arguments, self.objects):
            raise PermissionError("Cannot delete all collection items without a specific request.")

        print([x for x in q.run(coll.application.connection)])
        return q.delete(durability=self.durability, return_changes=self.return_changes).run(coll.application.connection)

    def get_document(self):
        doc = self.reference.get_document()
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