from collections import OrderedDict
from copy import copy, deepcopy
from functools import partial
import datetime

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


def list_meld(a, b):
    a_len = len(a)
    b_len = len(b)
    trunc_len = min(a_len, b_len)
    a_remainder = None if a_len < trunc_len else deepcopy(a[trunc_len:])
    b_remainder = None if b_len < trunc_len else deepcopy(b[trunc_len:])

    ret = []
    for i in range(trunc_len):
        x = a[i]
        y = b[i]
        if isinstance(x, dict) and isinstance(y, dict):
            ret.append(deep_merge(x, y, 'meld'))
        elif isinstance(x, list) and isinstance(y, list):
            ret.append(list_meld(x, y))
        else:
            ret.append(y)

    if a_remainder:
        ret.extend(a_remainder)
    if b_remainder:
        ret.extend(b_remainder)
    return ret


def deep_merge(a, b, list_merge_method='set'):
    """
    Merges dicts b into a, including expanding list items

    Args:
        a: dict
        b: dict
        list_merge_method: 'set', 'replace', 'extend', or 'meld'

    Returns:
        A deeply merged structure.

    """


    a = deepcopy(a)

    for key in b:
        if key not in a:
            a[key] = deepcopy(b[key])
        elif a[key] != b[key]:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                a[key] = deep_merge(a[key], b[key], list_merge_method)
            elif hasattr(a[key], '__iter__') and hasattr(b[key], '__iter__'):
                if list_merge_method == 'replace':
                    a[key] = deepcopy(b[key])
                if list_merge_method == 'set':
                    a[key] = list(set(a[key]).union(set(b[key])))
                elif list_merge_method == 'extend':
                    a[key].extend(deepcopy(b[key]))
                elif list_merge_method == 'meld':
                    a[key] = list_meld(a[key], b[key])
                else:
                    raise ValueError('list_expansion_method should be set, replace, extend. Was {0}'.format(
                        list_merge_method))
            else:
                a[key] = b[key]  # prefer b to a
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
    geo = partial(extend, {
        "type": "object",
        "geo": True,
        "properties": {
            "type": {"type": "string"},
            "coordinates": {"type": "array", "items": {"type": "number"}}}})
    array = partial(extend, {"type": "array"})
    integer = partial(extend, {"type": "integer"})
    number = partial(extend, {"type": "number"})
    boolean = partial(extend, {"type": "boolean"})
    date = partial(extend, {"type": "string", "format": "date-time"})
    color = partial(extend, {"type": "string", "formatters": "color"})
    datetime = partial(extend, {"type": "string", "format": "date-time"})
    creation_timestamp = partial(extend, {"type": "string", "format": "date-time", "on_creation": True})
    update_timestamp = partial(extend, {"type": "string", "format": "date-time", "on_update": True})
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
    null = partial(extend, {"type": "null"})

    @staticmethod
    def props(*args):
        properties = OrderedDict()
        for k, v in args:
            properties[k] = v

        return properties

    @staticmethod
    def fk(*args, **kwargs):
        if len(args) == 3:
            _, app, collection = args
        elif len(args) == 2:
            app, collection = args
        else:
            raise TypeError("Must provide at least app and collection to this function")
        return S.string({"type": "string", "fk": '/'.join([app, collection])}, **kwargs)

    @staticmethod
    def fk_array(*args, **kwargs):
        if len(args) == 3:
            _, app, collection = args
        elif len(args) == 2:
            app, collection = args
        else:
            raise TypeError("Must provide at least app and collection to this function")
        return S.array(items=S.fk(app, collection), **kwargs)

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

    @staticmethod
    def compose(*schemas):
        """
        Composes schemas in order, with subsequent schemas taking precedence over earlier ones.

        Args:
            *schemas: A list of schemas. Definitions may be included

        Returns:
            A JSON schema
        """
        result = OrderedDict()
        for s in schemas:
            result = deep_merge(result, s)
        return result