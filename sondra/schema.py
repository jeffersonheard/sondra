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

class s(object):
    base_object = {
        "type": "object",
        "properties": {},
    }

    string = partial(extend, {"type": "string"})
    array = partial(extend, {"type": "array"})
    integer = partial(extend, {"type": "integer"})
    number = partial(extend, {"type": "number"})
    boolean = partial(extend, {"type": "boolean"})



