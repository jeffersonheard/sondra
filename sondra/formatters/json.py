import json

from sondra import document
from sondra.utils import mapjson
from sondra.ref import Reference

class JSON(object):
    """
    This formats the API output as JSON. Used when ;formatters=json or ;json is a parameter on the last item of a URL.

    Optional arguments:

    * **indent** (int) - Formats the JSON output for human reading by inserting newlines and indenting ``indent`` spaces.
    * **fetch** (string) - A key in the document. Fetches the sub-document(s) associated with that key.
    * **ordered** (bool) - Sorts the keys in dictionary order.
    """
    # TODO make dotted keys work in the fetch parameter.

    def __call__(self, reference, results, **kwargs):

        # handle indent the same way python's json library does
        if 'indent' in kwargs:
            kwargs['indent'] = int(kwargs['indent'])

        if 'ordered' in kwargs:
            ordered = bool(kwargs.get('ordered', False))
            del kwargs['ordered']
        else:
            ordered = False

        # fetch a foreign key reference and append it as if it were part of the document.
        if 'fetch' in kwargs:
            fetch = kwargs['fetch'].split(',')
            del kwargs['fetch']
        else:
            fetch = []

        # note this is a closure around the fetch parameter. Consider before refactoring out of the method.
        def serialize(doc):
            if isinstance(doc, document.Document):
                ret = doc.json_repr(ordered=ordered)
                for f in fetch:
                    if f in ret:
                        if isinstance(doc[f], list):
                            ret[f] = [d.json_repr(ordered=ordered) for d in doc[f]]
                        elif isinstance(doc[f], dict):
                            ret[f] = {k: v.json_repr(ordered=ordered) for k, v in doc[f].items()}
                        else:
                            ret[f] = doc[f].json_repr(ordered=ordered)
                return ret
            else:
                return doc

        result = mapjson(serialize, results)  # make sure to serialize a full Document structure if we have one.

        if not (isinstance(result, dict) or isinstance(result, list)):
            result = {"_": result}

        return 'application/json', json.dumps(result, **kwargs)