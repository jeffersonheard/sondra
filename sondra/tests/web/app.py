from flask import Flask, Response
import os
import rethinkdb as r

from sondra.auth import Auth
from sondra.flask import api_tree

from sondra.tests.api import SimpleApp, ConcreteSuite, AuthenticatedApp, AuthorizedApp

app = Flask(__name__)
app.debug = True

app.suite = suite = ConcreteSuite()

api = SimpleApp(app.suite)
auth = Auth(app.suite)
AuthenticatedApp(app.suite)
AuthorizedApp(app.suite)
app.suite.ensure_database_objects()
# app.suite.clear_databases()

app.register_blueprint(api_tree, url_prefix='/api')

@app.route('/uploads/<path:locator>')
def serve_media(locator):
    print(locator)
    application, collection, filename = locator.split('/')
    uuid, orig_filename = filename.split(',', 1)
    file_record = r.db(application.replace('-','_')).table(collection.replace('-','_') + "__files").get(uuid).run(app.suite[application].connection)
    mimetype = file_record['metadata'].get('mimetype','application/x-octet-stream')

    def generator():
        with open(file_record['filename']) as stream:
            while True:
                chunk = stream.read(65336)
                if chunk:
                    yield chunk
                else:
                  break

    return Response(generator(), mimetype=mimetype)


if __name__ == '__main__':
    app.run()