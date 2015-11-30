class Help(object):
    name = 'help'

    def __call__(self, reference, result):
        value = reference.value
        if 'method' in reference.kind:
            return 'text/html', reference.suite.docstring_processor(value.help(value))
        else:
            return 'text/html', reference.suite.docstring_processor(value.help())

