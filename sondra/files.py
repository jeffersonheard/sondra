from functools import lru_cache
import os
import rethinkdb as r

from sondra.document.valuehandlers import ValueHandler

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


class FileHandler(ValueHandler):
    def __init__(self, storage_service, key, content_type='application/octet-stream'):
        self._storage_service = storage_service
        self._key = key
        self._content_type = content_type

    def post_save(self, document):
        self._storage_service.assoc(document, document.obj[self._key])

    def to_json_repr(self, value, document):
        if not hasattr(value, 'read'):
            return super().to_json_repr(value, document)
        else:
            return self._storage_service.store(
                document=document,
                key=self._key,
                original_filename=getattr(value, "filename", "uploaded-file.dat"),
                content_type=self._content_type,
                stream=value
            )

    def pre_delete(self, document):
        self._storage_service.delete_for_document(document)

    def to_python_repr(self, value, document):
        return self._storage_service.stream(value)

    def to_rql_repr(self, value, document):
        return super().to_rql_repr(value, document)


class FileStorageDefaults(object):
    """Suite mixin for suite containing defaults for file storage"""
    media_url_path = "media"


class FileStorageService(object):
    def __init__(self):
        self._suite = None
        self._media_url = None
        self._path_start = None

    def _db(self, collection):
        return collection.application.db

    def _conn(self, collection):
        return collection.application.connection

    @lru_cache()
    def _table_name(self, collection):
        return "_sondra_files__{collection}".format(collection=collection)

    @lru_cache()
    def _table(self, collection):
        db = self._db(collection)
        conn = self._conn(collection)
        table_name = self._table_name(collection)
        table = db.table(table_name)

        all_tables = { name for name in db.table_list().run(conn) }
        if table not in all_tables:
            db.table_create(table_name).run(conn)
            db.index_create(table_name, 'document').run(conn)
            db.index_create(table_name, 'collection').run(conn)

        return table

    def connect(self, suite):
        self._suite = suite

        host = "{scheme}://{netloc}".format(
            scheme=suite.base_url_scheme, netloc=suite.base_url_netloc)
        self._media_url = _join_components(host, suite.media_url_path)
        self._path_start = len(self._media_url) + 1

    def assoc(self, document, url):
        app, coll, pk_ext = url[self._path_start:].split('/', 2)
        pk, ext = os.path.splitext(pk_ext)
        self._table(document.collection).get(pk).update({"document": document.id}).run(self._conn(document.collection))

    def store(self, document, key, original_filename, content_type, stream):
        collection = document.collection
        if document.id is not None:
            self.delete_for_document(document, key)

        _, filename = os.path.split(original_filename)
        _, extension = os.path.splitext(filename)
        result = self._table(collection).insert({
            "collection": collection.name,
            "document": None,
            "key": key,
            "original_filename": filename,
            "extension": extension,
            "content_type": content_type,
        }).run(self._conn(collection))

        new_filename = "{id}.{ext}".format(id=result['generated_keys'][0], extension=extension)
        self.store_file(collection, new_filename, stream)
        return "{media_url}/{app}/{coll}/{new_filename}".format(
            media_url=self._media_url,
            app=collection.application.slug,
            coll=collection.slug,
            new_filename=new_filename
        )

    def stream_file(self, collection, ident_ext):
        raise NotImplementedError("Implement stream_file in a concrete class")

    def store_file(self, collection, ident_ext, stream):
        raise NotImplementedError("Implement store_stream in an concrete class")

    def delete_file(self, collection, ident_ext):
        raise NotImplementedError("Implement delete_file in a concrete class")

    def delete_from_collection(self, collection, ident):
        self.delete_file(collection, ident)
        self._table(collection).get(id).delete().run(self._conn)

    def delete_for_document(self, document, key=None):
        if key is not None:
            existing = self._table(document.collection)\
                .get_all(document, index='document')\
                .filter({'key': key})\
                .run(self._conn(document.collection))

            for f in existing:  # should only be one
                self.delete_file(document.collection, f['id'] + '.' + f['extension'])
        else:
            self._table(document.collection)\
                .get_all(document, index='document')\
                .delete()\
                .run(self._conn(document.collection))

    def stream(self, url):
        app, coll, pk = url[self._path_start:].split('/', 2)
        collection = self._suite[app][coll]
        record = self._table(collection).get(pk).run(self._conn(collection))
        in_stream = self.stream_file(collection, pk)
        return {
            "content_type": record['content_type'],
            "filename": record['original_filename'],
            "stream": in_stream
        }


class LocalFileStorageDefaults(FileStorageDefaults):
    """Suite mixin for local file storage defaults"""
    media_path = os.path.join(os.getcwd(), "_media")
    media_path_permissions = 0o755
    chunk_size = 16384


class LocalFileStorageService(FileStorageService):
    def __init__(self):
        super(LocalFileStorageService, self).__init__()

        self._root = None

    def connect(self, suite):
        super(LocalFileStorageService, self).connect(suite)

        self._root = suite.media_file_root \
            if suite.media_file_root.startswith('/') \
            else os.path.join(os.getcwd(), suite.media_file_root)

        os.makedirs(self._root, self._suite.media_path_perms, exist_ok=True)

    def _path(self, collection, make=False):
        p = os.path.join(self._root, collection.application.slug, collection.slug)
        if make:
            os.makedirs(p, exist_ok=True)
        return p

    def stream_file(self, collection, ident_ext):
        return open(os.path.join(self._path(collection), ident_ext))

    def delete_file(self, collection, ident_ext):
        os.unlink(os.path.join(self._path(collection), ident_ext))

    def store_file(self, collection, ident_ext, stream):
        p = self._path(collection, True)
        dest = os.path.join(p, ident_ext)
        with open(dest, 'w') as out:
            chunk = stream.read(self._suite.chunk_size)
            while chunk:
                out.write(chunk)
                chunk = stream.read(self._suite.chunk_size)
            out.flush()





