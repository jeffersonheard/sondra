import json

from sondra import collection, document
from sondra.document.schema_parser import Geometry
from sondra.utils import mapjson

class GeoJSON(object):
    def __call__(self, reference, result, **kwargs):
        if 'geom' in kwargs:
            geometry_field = kwargs['geom']
        else:
            geometry_field = None

        def fun(doc):
            if isinstance(doc, document.Document):
                if doc.specials:
                    for s, t in doc.specials.items():
                        if isinstance(t, Geometry):
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

        if 'indent' in kwargs:
            kwargs['indent'] = int(kwargs['indent'])

        if 'ordered' in kwargs:
            ordered = bool(kwargs.get('ordered', False))
            del kwargs['ordered']


        result = mapjson(fun, result)  # make sure to serialize a full Document structure if we have one.

        if isinstance(result, list):
            ret = {
                "type": "FeatureCollection",
                "features": result
            }
        else:
            ret = result
        return 'application/json', json.dumps(ret, indent=4)