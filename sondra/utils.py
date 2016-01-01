import importlib
import inspect
import re
from copy import deepcopy
import hashlib
import random
import time
import sys
from importlib import import_module


def split_camelcase(name):
    return re.sub('([A-Z]+)', r' \1', name).title().strip()


def convert_camelcase(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def camelcase_slugify(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1-\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1-\2', s1).lower()


def mapjson(fun, doc):
    if isinstance(doc, dict):
        return {k: mapjson(fun, v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [mapjson(fun, v) for v in doc]
    else:
        return fun(doc)


def qiter(o):
    if o is not None:
        for x in o:
            yield x
    else:
        raise StopIteration


def is_exposed(fun):
    return hasattr(fun, 'exposed')


def schema_with_properties(original, **updates):
    new_schema = deepcopy(original)
    new_schema['properties'].update(updates)
    return new_schema


def schema_sans_properties(original, *properties):
    new_schema = deepcopy(original)
    for property in (p for p in properties if p in new_schema['properties']):
        del new_schema['properties'][property]
    return new_schema


def schema_with_definitions(original, **updates):
    new_schema = deepcopy(original)
    new_schema['definitions'].update(updates)
    return new_schema


def schema_sans_definitions(original, *properties):
    new_schema = deepcopy(original)
    for property in (p for p in properties if p in new_schema['definitions']):
        del new_schema['definitions'][property]
    return new_schema


def resolve_class(obj, required_superclass=object, required_metaclass=type):
    if isinstance(obj, str):
        modulename, classname = obj.rsplit('.', 1)
        module = importlib.import_module(modulename)
        klass = getattr(module, classname)
    else:
        klass = obj

    if not issubclass(klass, required_superclass):
        raise TypeError("{0} is not of type {1}".format(
            klass.__name__,
            required_superclass.__name__
        ))

    if not isinstance(klass, required_metaclass):
        raise TypeError("{0} must use {1} metaclass".format(
            klass.__name__,
            required_metaclass.__name__
        ))

    return obj


# Use the system PRNG if possible
try:
    random = random.SystemRandom()
    using_sysrandom = True
except NotImplementedError:
    import warnings
    warnings.warn('A secure pseudo-random number generator is not available '
                  'on your system. Falling back to Mersenne Twister.')
    using_sysrandom = False

def get_random_string(length=12,
                      allowed_chars='abcdefghijklmnopqrstuvwxyz'
                                    'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'):
    """
    Adapted from Django. REMOVED SECRET_KEY FOR NOW

    Returns a securely generated random string.
    The default length of 12 with the a-z, A-Z, 0-9 character set returns
    a 71-bit value. log_2((26+26+10)^12) =~ 71 bits
    """
    if not using_sysrandom:
        # This is ugly, and a hack, but it makes things better than
        # the alternative of predictability. This re-seeds the PRNG
        # using a value that is hard for an attacker to predict, every
        # time a random string is required. This may change the
        # properties of the chosen random sequence slightly, but this
        # is better than absolute predictability.
        random.seed(
            hashlib.sha256(
                ("%s%s" % (
                    random.getstate(),
                    time.time()).encode('utf-8'))
            ).digest())
    return ''.join(random.choice(allowed_chars) for i in range(length))


def import_string(dotted_path):
    """
    Adapted from Django.

    Import a dotted module path and return the attribute/class designated by the
    last name in the path. Raise ImportError if the import failed.
    """
    try:
        module_path, class_name = dotted_path.rsplit('.', 1)
    except ValueError:
        msg = "%s doesn't look like a module path" % dotted_path
        raise(ImportError, ImportError(msg), sys.exc_info()[2])

    module = import_module(module_path)

    try:
        return getattr(module, class_name)
    except AttributeError:
        msg = 'Module "%s" does not define a "%s" attribute/class' % (
            module_path, class_name)
        six.reraise(ImportError, ImportError(msg), sys.exc_info()[2])