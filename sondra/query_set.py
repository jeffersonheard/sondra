import json
import rethinkdb as r

from sondra.exceptions import ValidationError

class QuerySet(object):
    """
    Limit the objects we are targeting in an API request
    """
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

    def __init__(self, coll):
        self.coll = coll

    def is_restricted(self, api_arguments, objects=None):
        """
        Determine if the user has requested specific objects or is limiting the objects by filter.

        :param api_arguments:
        :param objects:
        :return:
        """
        return objects or api_arguments.get('flt', None) or api_arguments.get('geo', None)

    def get_query(self, api_arguments, objects=None):
        """
        Apply all filters in turn and return a ReQL query.

        :param api_arguments: flt, geo, agg, start, end, limit filters
        :param objects: A list of object IDs.
        :return:
        """
        objects = objects or []
        if len(objects):
            if 'index' in api_arguments:
                q = self.coll.table.get_all(objects, index=api_arguments['index'])
            else:
                q = self.coll.table.get_all(objects)
        else:
            q = self.coll.table

        q = self._handle_simple_filters(api_arguments, q)
        q = self._handle_spatial_filters(self.coll, api_arguments, q)
        q = self._handle_aggregations(api_arguments, q)
        q = self._handle_limits(api_arguments, q)
        return q

    def __call__(self, api_arguments, objects=None):
        q = self.get_query(api_arguments, objects)
        return self.coll.q(q)

    def _handle_simple_filters(self, api_arguments, q):
        # handle simple filters
        if 'flt' in api_arguments:
            flt = json.loads(
                api_arguments['flt']) \
                    if isinstance(api_arguments['flt'], str) \
                    else api_arguments['flt']
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

    def _handle_spatial_filters(self, coll, api_arguments, q):
        # handle geospatial queries
        if 'geo' in api_arguments:
            print("Geospatial limit")
            geo = json.loads(
                api_arguments['geo']) \
                    if isinstance(api_arguments['geo'], str) \
                    else api_arguments['geo']

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

    def _handle_aggregations(self, api_arguments,  q):
        # handle aggregation queries
        if 'agg' in api_arguments:
            op = json.loads(api_arguments['agg'])
            if op['name'] not in self.SAFE_OPS:
                raise ValidationError("Cannot perform unsafe op in GET")
            q = getattr(q, op['name'])(*op.get('args',[]), **op.get('kwargs', {}))
        return q

    def _handle_limits(self, api_arguments, q):
        # handle start, limit, and end
        if 'start' and 'end' in api_arguments:
            s = api_arguments['start']
            e = api_arguments['end']
            if e == 0:
                q = q.skip(s)
            else:
                q = q.slice(s, e)
        else:
            if 'start' in api_arguments:
                s = api_arguments['start']
                q = q.skip(s)
            if 'limit' in api_arguments:
                limit = api_arguments['limit']
                q = q.limit(limit)
            #else:
            #    q = q.limit(self.MAX_RESULTS)
        return q