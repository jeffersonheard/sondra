"""
A Python client that gives symmetry to the way all APIs are called.  This allows you to make lookups and method calls
in the same way on remote services as you would on local collections, also enabling you to authenticate.

Currently very simple, but at least supports authentication.
"""

import requests
import json

class Client(object):
    def __init__(self, suite, url, auth_app="auth", auth=None):
        self._suite = suite
        self._log = suite.log
        self._apps = {}
        self._auth_token = None

        self.url = url
        self.auth_app = auth_app
        if auth:
            self.authorize(*auth)

        self._fetch_schema()

    @property
    def headers(self):
        return {} if not self._auth_token else {"Authorization", self._auth_token}

    def authorize(self, username, password):
        result = requests.post("{url}/{auth_app}.login".format(url=self.url, auth_app=self.auth_app), {"username": username, "password": password})
        if result.ok:
            self._log.info("Authorized API {url} with {username}".format(url=self.url, username=username))
            self._auth_token = result.text
        else:
            result.raise_for_status()

    def _fetch_schema(self):
        result = requests.get("{url};schema".format(url=self.url), headers=self.headers)
        if result.ok:
            schema = result.json()
            for app, url in schema['applications'].items():
                self._apps[app] = ApplicationClient(self, url)

    def __getitem__(self, key):
        return self._apps[key]


class MethodClient(object):
    def __init__(self, client, url, method_name):
        self.url = url + "." + method_name
        self.client = client
        self._fetch_schema()

    def _fetch_schema(self):
        result = requests.get("{url};schema".format(url=self.url), headers=self.client.headers)
        if result.ok:
            self.schema = result.json()
        else:
            result.raise_for_status()

    def __call__(self, *args, **kwargs):
        result = requests.post("url;json".format(url=self.url), headers=self.client.headers, data=json.dumps(kwargs))
        if result.ok:
            return result.json()
        else:
            result.raise_for_status()


class ApplicationClient(object):
    def __init__(self, client, url):
        self.collections = {}
        self.methods = {}
        self.client = client
        self.url = url
        self._fetch_schema()

    def _fetch_schema(self):
        result = requests.get("{url};schema".format(url=self.url), headers=self.client.headers)
        if result.ok:
            self.schema = result.json()
            for collection, url in self.schema['collections'].items():
                self.collections[collection] = CollectionClient(self, url)
            for method in self.schema['methods']:
                self.methods[method] = MethodClient(self.client, self.url, method)
        else:
            result.raise_for_status()

    def __getitem__(self, item):
        return self.collections[item]

    def __getattr__(self, item):
        """If the item is in the application's methods dictionary, return a methodcall"""
        if item not in self.methods:
            raise AttributeError(item)

        return self.methods[item]


class CollectionClient(object):
    def __init__(self, application, url):
        self.client = application.client
        self.methods = {}
        self.document_methods = {}
        self.application = application
        self.url = url

        self._fetch_schema()

    def _fetch_schema(self):
        result = requests.get("{url};schema".format(url=self.url), headers=self.client.headers)
        if result.ok:
            self.schema = result.json()
            for method in self.schema['methods']:
                self.methods[method] = MethodClient(self.client, self.url, method)
            for method in self.schema['document_methods']:
                self.document_methods[method] = MethodClient(self.client, self.url, method)
        else:
            result.raise_for_status()

    def __getitem__(self, item):
        return DocumentClient(self, item)

    def __setitem__(self, key, value):
        doc = DocumentClient(self, key, src=value)
        doc.save()

    def __delitem__(self, key):
        doc = DocumentClient(self, key)
        doc.delete()

    def __getattr__(self, item):
        if item not in self.methods:
            raise AttributeError(item)

        return self.methods[item]

    def append(self, value):
        result = requests.post("{url};json".format(url=self.url), headers=self.client.headers, data=json.dumps(value))
        if result.ok:
            return result.json()
        else:
            result.raise_for_status()

    def query(self, **query):
        result = requests.get("{url};json".format(url=self.url), headers=self.client.headers, data=query)
        if result.ok:
            return [DocumentClient(self, r['id'], r) for r in result.json()]
        else:
            result.raise_for_status()


class DocumentClient(object):
    def  __init__(self, collection, key, src=None):
        self.client = collection.client
        self.methods = collection.document_methods
        self.application = collection.application
        self.collection = collection
        self.url = self.collection.url = "/" + key
        self.key = key
        self.obj = src

    def fetch(self):
        result = requests.get("{url};json".format(url=self.url), headers=self.client.headers)
        if result.ok:
            self.obj = result
        else:
            result.raise_for_status()

    def __getitem__(self, item):
        if not self.obj:
            self.fetch()

        return self.obj[item]

    def __getattr__(self, item):
        if item not in self.methods:
            raise AttributeError(item)

        return self.methods[item]

    def save(self):
        result = requests.put("{url};json".format(url=self.url), headers=self.client.headers, data=json.dumps(self.obj))
        if result.ok:
            return result.json()
        else:
            result.raise_for_status()

    def delete(self):
        result = requests.delete("{url};json".format(url=self.url), headers=self.client.headers)
        if result.ok:
            return result.json()
        else:
            result.raise_for_status()


