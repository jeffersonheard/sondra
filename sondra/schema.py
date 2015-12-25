from copy import copy
from functools import partial


def extend(proto, *values, **kwargs):
    ret = copy(proto) if proto != {} else proto
    for v in values:
        ret.update(v)
    ret.update(kwargs)
    return ret


def remove(proto, *keys):
    ret = copy(proto)
    for key in keys:
        if key in ret:
            del ret[key]
    return ret


# fragments to extend.

class S(object):

    @staticmethod
    def object(properties=None, **kwargs):
        return extend({
            "type": "object",
            "properties": properties or {},
        }, **kwargs)

    string = partial(extend, {"type": "string"})
    array = partial(extend, {"type": "array"})
    integer = partial(extend, {"type": "integer"})
    number = partial(extend, {"type": "number"})
    boolean = partial(extend, {"type": "boolean"})
    date = partial(extend, {"type": "string", "format": "date"})
    color = partial(extend, {"type": "string", "format": "color"})
    datetime = partial(extend, {"type": "string", "format": "datetime"})
    datetime_local = partial(extend, {"type": "string", "format": "datetime-local"})
    email = partial(extend, {"type": "string", "format": "email"})
    month = partial(extend, {"type": "string", "format": "month"})
    range = partial(extend, {"type": "string", "format": "range"})
    tel = partial(extend, {"type": "string", "format": "tel"})
    text = partial(extend, {"type": "string", "format": "text"})
    textarea = partial(extend, {"type": "string", "format": "textarea"})
    time = partial(extend, {"type": "string", "format": "time"})
    url = partial(extend, {"type": "string", "format": "url"})
    week = partial(extend, {"type": "string", "format": "week"})

