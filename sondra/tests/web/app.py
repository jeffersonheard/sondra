from flask import Flask

from sondra.auth import Auth
from sondra.flask import api_tree

from sondra.tests.web import documents

app = Flask(__name__)
app.debug = True

app.suite = documents.ConcreteSuite()
api = documents.BaseApp(app.suite)
auth = Auth(app.suite)
auth.create_database()
auth.create_tables()
api.create_database()
api.create_tables()

app.register_blueprint(api_tree, url_prefix='/api')

if __name__ == '__main__':
    app.run()