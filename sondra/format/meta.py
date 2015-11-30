


class Formatter(object):
    _registry = {}

    def __init__(self):
        self._registry[self.name] = self

