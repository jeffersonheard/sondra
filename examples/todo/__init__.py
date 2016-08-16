from datetime import datetime

from sondra.document.valuehandlers import ListHandler, ForeignKey, DateTime

from sondra import auth
from sondra.api import expose
from sondra.application import Application
from sondra.collection import Collection
from sondra.document import Document, ListHandler
from sondra.document.processors import SlugPropertyProcessor, TimestampOnCreate, TimestampOnUpdate
from sondra.document.schema_parser import ForeignKey, DateTime
from sondra.lazy import ref, fk
from sondra.schema import S
from sondra.suite import Suite


class TodoSuite(Suite):
    pass


@auth.authorization_required('read','write')
class Card(Document):
    "A single item in the to-do list"

    schema = S.object({
        'title': S.string(title='Title', description="The title to display for the item"),
        'checked': S.boolean(default=False),
        'archived': S.boolean(default=False),
        'created': S.datetime(),
        'updated': S.datetime(),
        'description': S.string(title='Notes', description="Markdown for notes"),
        'notes': S.array(items=ref('self', 'note')),
        'attachments': S.array(items=S.url()),
        'images': S.array(items=S.url())
    })
    specials = {
        'created': DateTime(),
        'updated': DateTime()
    }
    processors = {
        'created': TimestampOnCreate(),
        'updated': TimestampOnUpdate(),
    }
    definitions = {
        'note': S.object({
            'content': S.string(),
            'created': S.datetime(),
            'updated': S.datetime()
        })
    }

    @auth.authorized_method
    @expose.expose_method
    def annotate(self, content):
        self['notes'].append({
            'content': S.string(),
            'created': datetime.now().isoformat(),
            'updated': datetime.now().isoformat()
        })


@auth.authorization_required('read','write')
class TodoList(Document):
    schema = S.object({
        'owner': fk(auth.Users),
        'title': S.string(title='Title'),
        'slug': S.string(),
        'created': S.datetime(),
        'updated': S.datetime(),
        'description': S.string(title='Description'),
        'cards': S.array(items=fk('sondra.examples.todo.Cards'))
    })
    specials = {
        'created': DateTime(),
        'updated': DateTime(),
        'cards': ListHandler(ForeignKey('todo','cards'))
    }
    processors = {
        'slug': SlugPropertyProcessor('title'),
        'created': TimestampOnCreate(),
        'updated': TimestampOnUpdate(),
    }


class TodoLists(Collection):
    primary_key = 'slug'
    document_class = TodoList


class Cards(Collection):
    document_class = Card


class Todo(Application):
    collections = (
        TodoLists,
        Cards
    )
