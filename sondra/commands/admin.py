import click
import importlib
import json
from sondra.auth import Auth, Users, Roles

suite = None
auth = None

@click.group()
@click.option("--config", "-c", envvar="SONDRA_SUITE")
def cli(config):
    global suite, auth
    modulename, classname = config.rsplit(".", 1)
    mod = importlib.import_module(modulename)
    suite = getattr(mod, classname)()
    auth = Auth(suite)


@cli.command()
@click.option("--username", "-u")
@click.option("--email", "-e", prompt=True)
@click.option("--password", "-p", prompt=True)
@click.option("--givenName", "-f", prompt="First Name")
@click.option("--familyName", "-l", prompt="Last Name")
@click.option('--locale', default='en-US')
def add_user(username, email, givenname, familyname, password, locale):
    auth['users'].create_user(
        username, password, email=email, givenName=givenname, familyName=familyname, locale=locale)

@cli.command()
@click.option("--username", "-u")
@click.option("--email", "-e", prompt=True)
@click.option("--password", "-p", prompt=True)
@click.option("--givenName", "-f", prompt="First Name", default=None)
@click.option("--familyName", "-l", prompt="Last Name", default=None)
@click.option('--locale', default='en-US')
def add_superuser(username, email, givenName, familyName, password, locale):
    auth['users'].create_user(
        username, password, email=email, givenName=givenName, familyName=familyName, locale=locale)

    new_user = auth['users'][username]
    new_user['admin'] = True
    new_user.save()


@cli.command()
@click.argument('username')
@click.argument('json_value')
def update_user(username, json_value):
    user = auth['users'][username]
    updates = json.loads(json_value)
    for k, v in updates.items():
        user[k] = v
    user.save()


@cli.command()
@click.argument('username')
def delete_user(username):
    del auth['users'][username]


@cli.command()
@click.argument('username')
@click.argument('roles', nargs=-1)
def add_user_roles(username, roles):
    u = auth['users'][username]
    old_roles = set(u['roles'])
    new_roles = old_roles.union(set(roles))
    u['roles'] = list(new_roles)
    u.save()


@cli.command()
@click.argument('username')
@click.argument('roles', nargs=-1)
def delete_user_roles(username, roles):
    u = auth['users'][username]
    old_roles = set(u['roles'])
    new_roles = old_roles.difference(set(roles))
    u['roles'] = list(new_roles)
    u.save()



@cli.command()
@click.argument('name')
@click.argument('includes', nargs=-1)
def create_role(name, includes):
    auth['roles'].create({
        "name": name,
        "includes": includes
    })


@cli.command()
@click.option("--role", "-r")
@click.option("--application", "-a")
@click.option("--collection", "-c", default=None)
@click.option("--document", "-c", default=None)
@click.option("--method", "-m", default=None)
@click.option("--read", default=True)
@click.option("--add", default=False)
@click.option("--update", default=False)
@click.option("--delete", default=False)
@click.option("--schema", default=False)
@click.option("--help", default=False)
def grant(role, application, collection, document, method, read, update, add, delete, schema, help):
    r = auth['roles'][role]

    app = suite[application]
    coll = app[collection] if collection else None
    doc = coll[document] if document else None

    if method:
        r.grant(application=app, collection=coll, document=doc, method=method)
    if read or add or update or delete:
        r.grant(application=app, collection=coll, document=doc, action='help')
        r.grant(application=app, collection=coll, document=doc, action='schema')
    if read:
        r.grant(application=app, collection=coll, document=doc, action='read')
    if add:
        r.grant(application=app, collection=coll, document=doc, action='add')
    if update:
        r.grant(application=app, collection=coll, document=doc, action='update')
    if delete:
        r.grant(application=app, collection=coll, document=doc, action='delete')
    if schema:
        r.grant(application=app, collection=coll, document=doc, action='schema')
    if help:
        r.grant(application=app, collection=coll, document=doc, action='help')


@cli.command()
@click.option("--role", "-r")
@click.option("--application", "-a")
@click.option("--collection", "-c", default=None)
@click.option("--document", "-c", default=None)
@click.option("--method", "-m", default=None)
@click.option("--read", default=True)
@click.option("--add", default=False)
@click.option("--update", default=False)
@click.option("--delete", default=False)
@click.option("--schema", default=False)
@click.option("--help", default=False)
def revoke(role, application, collection, document, method, read, update, add, delete, schema, help):
    r = auth['roles'][role]

    app = suite[application]
    coll = app[collection] if collection else None
    doc = coll[document] if document else None

    if method:
        r.revoke(application=app, collection=coll, document=doc, method=method)
    if read or add or update or delete:
        r.revoke(application=app, collection=coll, document=doc, action='help')
        r.revoke(application=app, collection=coll, document=doc, action='schema')
    if read:
        r.revoke(application=app, collection=coll, document=doc, action='read')
    if add:
        r.revoke(application=app, collection=coll, document=doc, action='add')
    if update:
        r.revoke(application=app, collection=coll, document=doc, action='update')
    if delete:
        r.revoke(application=app, collection=coll, document=doc, action='delete')
    if schema:
        r.revoke(application=app, collection=coll, document=doc, action='schema')
    if help:
        r.revoke(application=app, collection=coll, document=doc, action='help')


@cli.command()
@click.option("--role", "-r")
@click.option("--application", "-a")
@click.option("--collection", "-c", default=None)
@click.option("--document", "-c", default=None)
@click.option("--method", "-m", default=None)
@click.option("--read", default=True)
@click.option("--add", default=False)
@click.option("--update", default=False)
@click.option("--delete", default=False)
@click.option("--schema", default=False)
@click.option("--help", default=False)
def inherit(role, application, collection, document, method, read, update, add, delete, schema, help):
    r = auth['roles'][role]

    app = suite[application]
    coll = app[collection] if collection else None
    doc = coll[document] if document else None

    if method:
        r.inherit(application=app, collection=coll, document=doc, method=method)
    if read or add or update or delete:
        r.inherit(application=app, collection=coll, document=doc, action='help')
        r.inherit(application=app, collection=coll, document=doc, action='schema')
    if read:
        r.inherit(application=app, collection=coll, document=doc, action='read')
    if add:
        r.inherit(application=app, collection=coll, document=doc, action='add')
    if update:
        r.inherit(application=app, collection=coll, document=doc, action='update')
    if delete:
        r.inherit(application=app, collection=coll, document=doc, action='delete')
    if schema:
        r.inherit(application=app, collection=coll, document=doc, action='schema')
    if help:
        r.inherit(application=app, collection=coll, document=doc, action='help')


if __name__=='__main__':
    cli()