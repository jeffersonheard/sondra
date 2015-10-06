from flask import Flask

from sondra.auth import Auth
from sondra.flask import api_tree

from sondra_webtests import docs

app = Flask(__name__)
app.debug = True

app.suite = docs.ConcreteSuite()
api = docs.BaseApp(app.suite)
auth = Auth(app.suite)
auth.create_database()
auth.create_tables()
api.create_database()
api.create_tables()

app.register_blueprint(api_tree, url_prefix='/api')

if __name__ == '__main__':
    app.run()