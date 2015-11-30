import json

from sondra import document
from sondra.utils import mapjson

class JSONFormatter(object):
    name = 'json'

    def __call__(self, reference, results, **kwargs):
        if 'indent' in kwargs:
            kwargs['indent'] = int(kwargs['indent'])

        def fun(doc):
            if isinstance(doc, document.Document):
                return mapjson(fun, doc.collection.json(doc))
            else:
                return doc

        result = mapjson(fun, results)  # make sure to serialize a full Document structure if we have one.

        if not (isinstance(result, dict) or isinstance(result, list)):
            result, = {"_": result}

        return 'application/json', json.dumps(result, indent=4)