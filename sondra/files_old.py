# Experimental file support.  Maybe this should just be a document processor...

import os
import rethinkdb as r
from functools import partial

from sondra.utils import get_random_string, mapjson
from sondra.document import signals as document_signals
from sondra.application import signals as app_signals

try:
    from werkzeug.utils import secure_filename
except ImportError:
    import re

    def secure_filename(name):
        name = re.sub(r'\s+', '-', name)  # Replace white space with dash
        name = name.sub(r'([a-zA-Z]):\\', '')
        return name.sub(r'[^a-zA-Z0-9\-.]+', '_', name)  # Replace non alphanumerics with a single _


def _strip_slashes(p):
    p = p[1:] if p.startswith('/') else p
    return p[:-1] if p.endswith('/') else p


def _join_components(*paths):
    return '/'.join(_strip_slashes(p) for p in paths)


def _persist(storage, document, value):
    if hasattr(value, 'read') and callable(value.read):
        return storage.create(document, value)
    else:
        return value


class FileStorage(object):
    chunk_size = 16384  # amount of data to read in at once

    @classmethod
    def configured(cls, *args, **kwargs):
        return (lambda collection: cls(collection, *args, **kwargs))

    def __init__(self, collection):
        self._collection = collection
        self._conn = self._collection.application.connection
        self._db = self._collection.application.db
        self._table_name = "sondra__{collection}_filestorage".format(collection=self._collection.name)
        self._table = r.db(self._db).table(self._table_name)
        self.ensure()
        self._connect_signals()

    def _connect_signals(self):
        def _delete_before_database_drop(sender, instance, **kwargs):
            if instance.slug == self._collection.application.slug:
                self.drop()

        self._app_pre_delete_database_receiver = app_signals.pre_delete_database.connect(_delete_before_database_drop)

        def _delete_document_files(sender, instance, **kwargs):
            if instance.id and instance.collection and (instance.collection.slug == self._collection.slug):
                self.delete_records_for_document(instance)

        self._doc_pre_delete_document_receiver = document_signals.pre_delete.connect(_delete_document_files)

    def __del__(self):
        app_signals.pre_delete_database.disconnect(self._app_pre_delete_database_receiver)
        document_signals.pre_delete.disconnect(self._doc_pre_delete_document_receiver)

    def persist_document_files(self, document):
        document.obj = mapjson(partial(_persist, self, document), document.obj)
        return document

    def ensure(self):
        try:
            self._db.table_create(self._table_name).run(self._conn)
            self._table.index_create('document').run(self._conn)
            self._table.index_create('url').run(self._conn)
        except r.ReqlError:
            pass  # fixme log exception

    def drop(self):
        for rec in self._table.run(self._conn):
            self.delete(rec)
        self._db.table_drop('sondra__file_storage_meta').run(self._conn)

    def clear(self):
        self.drop()
        self.ensure()

    def create(self, document, from_file):
        name = self.get_available_name(from_file.filename)

        record = {
            "original_filename": from_file.filename,
            "stored_filename": name,
            "url": self.get_url(name),
            "size": None,
            "document": document.id
        }

        record = self.store(record, from_file)
        self._table.insert(record)
        return record['url']

    def record_for_url(self, url):
        try:
            return next(self._table.get_all(url, index='url').run(self._conn))
        except r.ReqlError:
            return None

    def delete_records_for_document(self, doc):
        return self._table.get_all(doc.id, index='document').delete().run(self._conn)

    def delete_record(self, url):
        self._table.get_all(url, index='url').delete().run(self._conn)

    def save_record(self, record):
        self._table.insert(record, conflict='replace')

    def store(self, record, from_file):
        raise NotImplementedError("Must implement store() in a non-abstract class")

    def fetch(self, record):
        raise NotImplementedError("Must implement fetch() in a non-abstract class")

    def stream(self, record):
        raise NotImplementedError("Must implement stream() in a non-abstract class")

    def delete(self, record):
        raise NotImplementedError("Must implement delete() in a non-abstract class")

    def exists(self, filename):
        raise NotImplementedError("Must implement exists() in a non-abstract class")

    def replace(self, record, from_file):
        self.delete(record)
        record = self.store(record, from_file)
        self.save_record(record)

    def get_available_name(self, filename):
        filename_candidate = secure_filename(filename)
        if self.exists(filename_candidate):
            _original = filename_candidate
            while self.exists(filename_candidate):
                filename_candidate = _original + get_random_string()

    def get_url(self, name):
        raise NotImplementedError("Must implement get_url() in a non-abstract class")


class LocalStorage(FileStorage):
    def __init__(self, upload_path=None, media_url="uploads", *args, **kwargs):
        super(LocalStorage, self).__init__(*args, **kwargs)

        suite = self._collection.suite
        host = "{scheme}://{netloc}".format(
            scheme=suite.base_url_scheme, netloc=suite.base_url_netloc)
        self._media_url = _join_components(host, media_url, self._collection.application.slug, self._collection.slug)
        self._storage_root = upload_path or getattr(suite, 'file_storage_path', os.path.join(os.getcwd(), 'media'))
        self._storage_path = os.path.join(self._storage_root, self._collection.application.slug, self._collection.slug)

    def _disk_name(self, record):
        return os.path.join(self._storage_path, record['stored_filename'])

    def ensure(self):
        super(LocalStorage, self).ensure()

        if not os.path.exists(self._storage_path):
            os.makedirs(self._storage_path)

    def get_url(self, name):
        return _join_components(self._media_url, name)

    def store(self, record, from_file):
        size = 0

        with open(self._disk_name(record), 'w') as output:
            while True:
                chunk = from_file.read(self.chunk_size)
                if chunk:
                    output.write(chunk)
                    size += len(chunk)
                else:
                    break
            output.flush()

        record['size'] = size
        return record

    def exists(self, filename):
        return os.path.exists(os.path.join(self._storage_path, filename))

    def delete(self, record):
        disk_name = self._disk_name(record)
        if os.path.exists(disk_name):
            os.unlink(disk_name)
        else:
            raise FileNotFoundError(record['original_filename'])
        self.delete_record(record['url'])

    def stream(self, record):
        return open(self._disk_name(record))

    def fetch(self, record):
        pass

