from collections import OrderedDict
from copy import copy, deepcopy
from functools import partial


def merge(a, b, path=None):
    "merges b into a"

    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass # same leaf value
            else:
                a[key] = b[key]  # prefer b to a
        else:
            a[key] = b[key]
    return a


def extend(proto, *values, **kwargs):
    ret = deepcopy(proto) if proto else OrderedDict()
    for v in values:
        ret.update(v)
    ret.update(kwargs)
    return ret


def remove(proto, *keys):
    ret = deepcopy(proto)
    for key in keys:
        if key in ret:
            del ret[key]
    return ret


# fragments to extend.

class S(object):
    @staticmethod
    def object(properties=None, **kwargs):
        properties = properties or OrderedDict()
        for pname, pschema in properties.items():
            if 'title' not in pschema:
                pschema['title'] = pname.replace('_', ' ').title()
        ret = extend(OrderedDict(), {
            "type": "object",
            "properties": properties,
        }, **kwargs)
        return ret

    string = partial(extend, {"type": "string"})
    file = partial(extend, {"type": "string", "file": True})
    image = partial(extend, {"type": "string", "image": True})
    geo = partial(extend, {"type": "object", "geo": True})
    array = partial(extend, {"type": "array"})
    integer = partial(extend, {"type": "integer"})
    number = partial(extend, {"type": "number"})
    boolean = partial(extend, {"type": "boolean"})
    date = partial(extend, {"type": "string", "format": "date-time"})
    color = partial(extend, {"type": "string", "formatters": "color"})
    datetime = partial(extend, {"type": "string", "format": "date-time"})
    datetime_local = partial(extend, {"type": "string", "formatters": "datetime-local"})
    email = partial(extend, {"type": "string", "formatters": "email"})
    month = partial(extend, {"type": "string", "formatters": "month"})
    range = partial(extend, {"type": "string", "formatters": "range"})
    tel = partial(extend, {"type": "string", "formatters": "tel"})
    text = partial(extend, {"type": "string", "formatters": "text"})
    textarea = partial(extend, {"type": "string", "formatters": "textarea", "long": True})
    time = partial(extend, {"type": "string", "formatters": "time"})
    url = partial(extend, {"type": "string", "formatters": "url"})
    week = partial(extend, {"type": "string", "formatters": "week"})

    @staticmethod
    def props(*args):
        properties = OrderedDict()
        for k, v in args:
            properties[k] = v

        return properties

    @staticmethod
    def fk(suite, app, collection, **kwargs):
        return S.string({"type": "string", "fk": '/'.join([app, collection])}, **kwargs)

    @staticmethod
    def fk_array(suite, app, collection, **kwargs):
        return S.array(items=S.fk(suite, app, collection), **kwargs)

    @staticmethod
    def external_key(url, **kwargs):
        return S.string({"type": "string", "fk": url}, **kwargs)

    @staticmethod
    def ref(definition, **kwargs):
        url = "#/definitions/{definition}".format(**locals())
        return extend({"$ref": url}, kwargs)

    @staticmethod
    def ref_array(definition, **kwargs):
        return S.array(items=S.ref(definition), **kwargs)

    @staticmethod
    def foreign_ref(suite, app, collection, definition, **kwargs):
        url = '/'.join((suite, app, collection, "#/definitions/{definition}".format(**locals())))
        return extend({"$ref": url, "suite": suite, "app": app, "collection": collection}, kwargs)

    def external_ref(self, url, **kwargs):
        return extend({"$ref": url}, kwargs)

    @staticmethod
    def nullable(o):
        if isinstance(o.get('type', 'string'), list):
            o['type'].append('null')
        else:
            o['type'] = [o.get('type', 'string'), 'null']

        return o


