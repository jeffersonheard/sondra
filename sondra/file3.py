"""
One more go at simple file storage.

Files are valid values when:

* posting an update to a/many document(s)
* storing documents programmatically
* calling methods on documents, collections, applications, and the suite.

Files should be deleted when records are deleted.

Sequence:

* API call incoming
* Check for permission to

Notes:

* Make sure to make this processor run AFTER any authentication, otherwise you could get lots of
  temp files littering the filesystem.

"""
from collections import defaultdict
from urllib.parse import urlparse

from werkzeug.utils import secure_filename

from sondra.api import RequestProcessor
import rethinkdb as r
from uuid import uuid4
import os
from flask import request

from sondra.document.valuehandlers import ValueHandler


class LocalFileStorage(object):
    chunk_size = 65356

    class File(object):
        """
        A read only file object. To write a file, use FileStorage.save()
        """

        def __init__(self, filename, orig_filename, url, **kwargs):
            self.filename = filename
            self.orig_filename = orig_filename
            self.url = url
            self._stream = None


        def __enter__(self):
            self._stream = open(self.filename, 'rb')
            return self._stream

        def __exit__(self, exc_type, exc_val, exc_tb):
            self._stream.close()


    def __init__(self, root, url_root, chunk_size=None):
        self.root = root
        self.url_root = url_root
        self.chunk_size = chunk_size or LocalFileStorage.chunk_size
        self._table_list = defaultdict(set)

    def _path(self, collection):
        p = os.path.join(
            self.root,
            collection.application.slug,
            collection.slug,
        )
        os.makedirs(p, exist_ok=True)
        return p

    def ensure_table(self, collection):
        file_list_name = collection.name + "__files"

        app_table_list = self._table_list[collection.application.name]
        if file_list_name not in app_table_list:
            try:
                r.db(collection.application.db).table_create(file_list_name).run(collection.application.connection)
                r.db(collection.application.db).index_create('url').run(collection.application.connection)
                app_table_list.add(file_list_name)
            except Exception as e:
                print(e)
        return r.db(collection.application.db).table(file_list_name)

    def save(self, file_obj, orig_filename, collection, **meta):
        filename = uuid4().hex
        p = self._path(collection)
        url = os.path.join(
            self.url_root,
            collection.application.slug,
            collection.slug,
            filename + ',' + secure_filename(orig_filename)
        )
        full_path = os.path.join(p, filename)

        with open(full_path, 'wb') as dest:
            size = 0
            while True:
                data = file_obj.read(self.chunk_size)
                if not data:
                    break
                else:
                    size += len(data)
                    dest.write(data)

        db_record = {
            "id": filename,
            "filename": full_path,
            "orig_filename": orig_filename,
            "url": url,
            "refs": 1,
            "created": r.now(),
            "size": size,
            "metadata": meta,
        }

        self.ensure_table(collection)\
            .insert(db_record)\
            .run(collection.application.connection)

        return url

    def delete(self, collection, url):
        __, filename = url.rsplit('/', 1)
        uuid, __  = filename.split(',', 1)

        db_record = r.db(collection.application.db).table(collection.name + "__files")\
            .get(uuid)\
            .update({'refs': r.row['refs']-1}, return_changes=True)\
            .run(collection.application.connection)

        if len(db_record['changes']):
            if db_record['changes'][0]['new_val']['refs'] == 0:
                os.unlink(db_record['changes'][0]['new_val']['full_pathname'])
                r.db(collection.application.db).table(collection.name + "__files")\
                    .get(uuid)\
                    .delete()\
                    .run(collection.application.connection)

    def get(self, suite, url):
        try:
            p_url = urlparse(url)
            *root, app, collection, filename = p_url.path.split('/')
            app_name = app.replace('-','_')
            collection_name = collection.replace('-','_')

            db_record = r.db(app_name).table(collection_name + "__files")\
                .get_all(url, index='url')\
                .run(suite[app].connection)

            if db_record:
                return LocalFileStorage.File(**db_record[0])
            else:
                return None
        except Exception as e:
            raise FileNotFoundError(str(e))


class FileUploadProcessor(RequestProcessor):
    def process_api_request(self, rq):
        if rq.files:
            if len(rq.objects) > 1:
                return self.process_multiple_objects_files(rq)
            else:
                return self.process_object_files(rq)
        else:
            return rq

    def process_object_files(self, rq):
        storage = rq.suite.file_storage
        for key, v in rq.files.items():
            rq.objects[0][key] = storage.save(v, v.filename, rq.reference.get_collection(), mimetype=v.mimetype)
        return rq

    def process_multiple_objects_files(self, rq):
        storage = rq.suite.file_storage
        for k, v in rq.files.items():
            index, key = k.split(':', 1)
            rq.objects[int(index)][key] = storage.save(
                v, v.filename, rq.reference.get_collection(), mimetype=v.mimetype)
        return rq


class FileHandler(ValueHandler):
    def __init__(self, key, content_type='application/octet-stream'):
        self._key = key
        self._content_type = content_type

    def to_json_repr(self, value, document):
        if not hasattr(value, 'read'):
            return super(FileHandler, self).to_json_repr(value, document)
        else:
            return document.suite.file_storage.save(
                value,
                document=document,
                key=self._key,
                original_filename=getattr(value, "filename", "uploaded-file.dat"),
                content_type=self._content_type,
            )

    def pre_delete(self, document):
        if document.get(self._key):
            document.suite.file_storage.delete(document.collection, document[self._key])

    def to_python_repr(self, value, document):
        return document.suite.file_storage.get(document.suite, value)

    def to_rql_repr(self, value, document):
        if not hasattr(value, 'read'):
            return super().to_rql_repr(value, document)
        else:
            return document.suite.file_storage.save(
                value,
                document=document,
                key=self._key,
                original_filename=getattr(value, "filename", "uploaded-file.dat"),
                content_type=self._content_type,
            )