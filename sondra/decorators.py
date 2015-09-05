from copy import copy
import re
import inspect
from .document import Collection, Document
from .ref import Reference

class ParseError(Exception):
    pass

def _schema(method):
    return {
        "id": method.__self__.url + '.' + method.slug + ';schema',
        "description": method.__doc__,
        "type": "object",
        "oneOf": ["#/definitions/request", "#/definitions/response"],
        "definitions": {
            "request": method.request_schema(method),
            "response": method.response_schema(method)
        }
    }

def _response_schema(method):
    # parse the return schema
    metadata = inspect.signature(method)
    instance = method.__self__
    if metadata.return_annotation is not metadata.empty:
        argtype = _parse_arg(instance, metadata.return_annotation)
        if 'type' in argtype:
            if argtype['type'] in {'list', 'object'}:
                return argtype
        elif "$ref" in argtype:
            coll = Reference(argtype['$ref'])
            return coll.get_collection().schema
        else:
            return {
                "type": "object",
                "properties": {
                    "v": argtype
                }
            }
    else:
        return {"type": None}


def _request_schema(method):
    required_args = []
    metadata = inspect.signature(method)
    instance = method.__self__
    properties = {}

    for i, (name, param) in enumerate(metadata.parameters.items()):
        if i == 0:
            continue
        schema = _parse_arg(instance, param.annotation)
        if param.default:
            schema['default'] = param.default
        else:
            required_args.append(name)
        properties[name] = schema

    return {
        "type": "object",
        "required": required_args,
        "properties": properties
    }


def _parse_arg(instance, arg):
    if isinstance(arg, tuple):
        arg, description = arg
    else:
        description = None

    if isinstance(arg, str):
        if arg in {'object', 'int', 'float', 'number', 'array'}:
            arg = {"type": arg}
        else:
            raise ParseError("string args must be of type object, int, float, number, array. Got {0}".format(arg))
    elif arg is str:
        arg = {"type": "string"}
    elif arg is int:
        arg = {"type": "int"}
    elif arg is float:
        arg = {"type": "number"}
    elif arg is bool:
        arg = {"type": "boolean"}
    elif arg is list:
        arg = {"type": "array"}
    elif arg is dict:
        arg = {"type": "object"}
    elif isinstance(arg, re.RegexObject):
        arg = {"type": "str", "pattern": arg.pattern}
    elif isinstance(arg, list):
        arg = {"type": "array", "items": _parse_arg(instance, arg[0])}
    elif isinstance(arg, dict):
        arg = {"type": "object", "properties": {k: _parse_arg(instance, v) for k, v in arg.items()}}
    elif issubclass(arg, Collection):
        arg = {"$ref": instance.application[arg.slug].url + ";schema"}
    elif issubclass(arg, Document):
        arg = copy(arg.schema)
        arg['id'] = arg.__module__ + "." + arg.__class__.__name__
    else:
        raise ParseError("arg types must be str, int, float, bool, list, dict, a Collection subclass or a compound of.")

    if description:
        arg['description'] = description
    return arg


def expose(method, authorization=None, modifies_args=()):
    """Defines a method that is exposable as an API call on the defining class.

    This method parses function annotations to determine the schema of arguments and returns. All
    exposed methods MUST have all arguments (except self) specified as given here. Valid parameter
    annotations::

        @exposable(permissions=('my_object.write'))
        def attach_object(
            self, x: float,
            y: float,
            obj: "/my_app/some_collection",
            freeze: bool=False
        ) -> None:
            ...
            ...
            ...

    Keyword arguments and variable length arguments are not supported.

    Args:
        permissions {str}: A set of permission names, one of which the user must have to execute
            this method.
        modifies_args={str}: The names of any Document arguments that the method might modify. This
            helps the API do permissions checking.
    """

    method.authorize = authorization or (lambda request, user: True)
    method.modifies_args = modifies_args
    method.request_schema = _request_schema
    method.response_schema = _response_schema
    method.schema = _schema
    method.slug = method.__name__.replace('_','-')
    method.exposed = True

    return method


def expose_methods(*method_tuples):
    """Class decorator that exposes existing methods on a class.

    Args:
        method_tuples [tuple]: A list of tuples of (method_name, argument_type(s), return_type)

    """