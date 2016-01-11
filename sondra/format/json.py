import json

from sondra import document
from sondra.utils import mapjson
from sondra.ref import Reference

class JSONFormatter(object):
    name = 'json'

    def _deref(self, suite, src, property):
        *props, i = property.split('.')
        for prop in props:
            if prop in src:
                src = src[prop]
            else:
                return
        print("args:", src, i)
        if i in src and src[i] is not None:
            x = Reference(suite, src[i]).value
            src[i] = x.collection.json(x)

    def __call__(self, reference, results, **kwargs):
        if 'indent' in kwargs:
            kwargs['indent'] = int(kwargs['indent'])

        if 'fetch' in kwargs:
            fetch = kwargs['fetch'].split(',')
            print("Fetch: ", fetch)
            del kwargs['fetch']
        else:
            fetch = []

        def fun(doc):
            if isinstance(doc, document.Document):
                ret = doc.collection.json(doc)
                for f in fetch:
                    self._deref(reference.environment, ret, f)
                return mapjson(fun, ret)
            else:
                return doc

        result = mapjson(fun, results)  # make sure to serialize a full Document structure if we have one.

        if not (isinstance(result, dict) or isinstance(result, list)):
            result, = {"_": result}

        return 'application/json', json.dumps(result, **kwargs)