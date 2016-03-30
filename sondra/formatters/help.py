from sondra.expose import method_help

class Help(object):
    def __call__(self, reference, result):
        value = reference.value
        if 'method' in reference.kind:
            return 'text/html', reference.value[0].suite.docstring_processor(method_help(*reference.value))
        else:
            return 'text/html', reference.value.suite.docstring_processor(value.help())

