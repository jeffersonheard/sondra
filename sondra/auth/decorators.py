def authorized_method(o):
    o.authentication_required = [o.slug]
    o.authorization_required = [o.slug]
    return o


def authenticated_method(o):
    o.authentication_required = [o.slug]
    return o


class authorization_required(object):
    def __init__(self, *protected):
        self.protected = protected

    def __call__(self, cls):
        cls.authentication_required = self.protected
        cls.authorization_required = self.protected
        return cls


class authentication_required(object):
    def __init__(self, *protected):
        self.protected = protected

    def __call__(self, cls):
        cls.authentication_required = self.protected
        return cls
