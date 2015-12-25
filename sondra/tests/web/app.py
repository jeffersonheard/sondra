from flask import Flask

from sondra.auth import Auth
from sondra.flask import api_tree

from sondra.tests.api import SimpleApp, ConcreteSuite

app = Flask(__name__)
app.debug = True

app.suite = ConcreteSuite()
api = SimpleApp(app.suite)
auth = Auth(app.suite)
# app.suite.clear_databases()

app.register_blueprint(api_tree, url_prefix='/api')

if __name__ == '__main__':
    app.run()