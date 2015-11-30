import json

class Schema(object):
    name = 'schema'

    def __call__(self, reference, result, **kwargs):
        if 'indent' in kwargs:
            kwargs['indent'] = int(kwargs['indent'])

        if 'method' in reference.kind:
            return 'text/html', json.dumps(reference.value.schema, **kwargs)
        else:
            return 'text/html', json.dumps(reference.value.schema, **kwargs)

