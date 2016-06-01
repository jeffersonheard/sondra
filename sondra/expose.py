from copy import copy
import io
import re
import inspect
from sondra.exceptions import ParseError
from sondra import help
from functools import wraps
from copy import deepcopy


def expose_method(method):
    method.exposed = True
    method.slug = method.__name__.replace('_','-')
    return method


def expose_method_explicit(request_schema=None, response_schema=None, side_effects=False, title=None, description=None):
    request_schema = request_schema or {'type': 'null'}
    response_schema = response_schema or {'type': 'null'}

    def expose_method_decorator(func):

        @wraps(func)
        def func_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        func_wrapper.exposed = True
        func_wrapper.slug = func.__name__.replace('_', '-')

        # auto-fill request schema items based on metadata if they were not explicitly provided.
        req_schema = deepcopy(request_schema)
        if 'title' not in req_schema:
            req_schema['title'] = title or func.__name__
        if 'description' not in req_schema:
            req_schema['description'] = req_schema.get('description', description or func.__doc__ or '*No description provided*')
        req_schema['side_effects'] = side_effects

        rsp_schema = deepcopy(response_schema)
        if 'title' not in rsp_schema:
            rsp_schema['title'] = title or func.__name__
        if 'description' not in rsp_schema:
            rsp_schema['description'] = rsp_schema.get('description', description or func.__doc__ or '*No description provided*')

        func_wrapper.title = title or func.__name__
        func_wrapper.request_schema = req_schema
        func_wrapper.response_schema = rsp_schema

        @wraps(func)
        def invoke(*args, request=None):
            func(*args, **request)

        func_wrapper.invoke = invoke

        return func_wrapper

    return expose_method_decorator


def method_url(instance, method):
    return instance.url + '.' + method.slug if instance is not None else method.slug


def method_schema(instance, method):
    id = method.slug
    if instance is not None:
        if instance:
            id = instance.url
    else:
        id = "*" + method.slug

    return {
        "id": id,
        "title": getattr(method, 'title', method.__name__),
        "description": method.__doc__ or "*No description provided*",
        "oneOf": [{"$ref": "#/definitions/method_request"}, {"$ref": "#/definitions/method_response"}],
        "definitions": {
            "method_request": method_request_schema(instance, method),
            "method_response": method_response_schema(instance, method)
        }
    }


def method_response_schema(instance, method):
    if hasattr(method, 'response_schema'):
        return method.response_schema

    # parse the return schema
    metadata = inspect.signature(method)
    if metadata.return_annotation is not metadata.empty:
        argtype = _parse_arg(instance, metadata.return_annotation)
        if 'type' in argtype:
            if argtype['type'] in {'list', 'object'}:
                return argtype
            else:
                return {
                    "type": "object",
                    "properties": {
                        "_": argtype
                    }
                }
        elif "$ref" in argtype:
            return argtype
        else:
            return {
                "type": "object",
                "properties": {
                    "_": argtype
                }
            }
    else:
        return {"type": "object", "description": "no return value."}


def method_request_schema(instance, method):
    if hasattr(method, 'request_schema'):
        return method.request_schema

    required_args = []
    metadata = inspect.signature(method)
    properties = {}

    for i, (name, param) in enumerate(metadata.parameters.items()):
        if name.startswith('_'):
            continue  # skips parameters filled in by decorators

        # if i == 0:  # skip the first arg.
        #     continue

        schema = _parse_arg(instance, param.annotation)
        if param.default is not metadata.empty:
            schema['default'] = param.default
        else:
            required_args.append(name)
        properties[name] = schema

    ret = {
        "type": "object",
        "properties": properties
    }
    if required_args:
        ret['required'] = required_args
    return ret


def _parse_arg(instance, arg):
    from sondra.document import Document
    from sondra.collection import Collection

    if isinstance(arg, tuple):
        arg, description = arg
    else:
        description = None

    if arg is None:
        return {"type": "null"}
    if isinstance(arg, str):
        arg = {"type": "string", "foreignKey": arg}
    elif arg is str:
        arg = {"type": "string"}
    elif arg is bytes:
        arg = {"type": "string", "formatters": "attachment"}
    elif arg is int:
        arg = {"type": "integer"}
    elif arg is float:
        arg = {"type": "number"}
    elif arg is bool:
        arg = {"type": "boolean"}
    elif arg is list:
        arg = {"type": "array"}
    elif arg is dict:
        arg = {"type": "object"}
    elif isinstance(arg, re._pattern_type):
        arg = {"type": "string", "pattern": arg.pattern}
    elif isinstance(arg, list):
        arg = {"type": "array", "items": _parse_arg(instance, arg[0])}
    elif isinstance(arg, dict):
        arg = {"type": "object", "properties": {k: _parse_arg(instance, v) for k, v in arg.items()}}
    elif issubclass(arg, Collection):
        arg = {"$ref": (instance.application[arg.slug].url if instance is not None else "<application>") + ";schema"}
    elif issubclass(arg, Document):
        arg = copy(arg.schema)
    else:
        arg = {"type": ['string','boolean','integer','number','array','object'], "description": "Unspecified type arg."}

    if description:
        arg['description'] = description

    return arg


def method_help(instance, method, out=None, initial_heading_level=0):
    out = out or io.StringIO()
    builder = help.SchemaHelpBuilder(
        method_schema(instance, method),
        out=out,
        initial_heading_level=0
    )
    builder.build()
    builder.line()
    return out.getvalue()