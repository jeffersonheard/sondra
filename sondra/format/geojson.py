import json

from sondra import collection, document
from sondra.utils import mapjson

class GeoJSON(object):
    name = 'geojson'

    def __call__(self, reference, result, **kwargs):
        def fun(doc):
            if isinstance(doc, document.Document):
                if doc.collection and doc.collection.specials:
                    for s, t in doc.collection.specials.items():
                        if isinstance(t, collection.Geometry):
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

        result = mapjson(fun, result)  # make sure to serialize a full Document structure if we have one.

        if isinstance(result, list):
            ret = {
                "type": "FeatureCollection",
                "features": result
            }
        else:
            ret = result
        return 'application/json', json.dumps(ret, indent=4)