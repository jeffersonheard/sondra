from sondra.collection import Collection
from sondra.document import Document
from sondra.application import Application
from sondra.suite import Suite
from sondra.schema import S

class Item(Document):
    schema = S.object(
        required=['title'],
        properties=S.props(
            ("title", S.string(description="The title of the item")),
            ("complete", S.boolean(default=False)),
            ("created", S.datetime()),
        ))

class Items(Collection):
    document_class = Item
    indexes = ["title", "complete"]
    order_by = ["created"]

class TodoApp(Application):
    collections = (Items,)

class TodoSuite(Suite):
    cross_origin = True
    debug = True


def todo_session():
    todo = TodoSuite("sondra_ex_")
    TodoApp(todo)

    todo.validate()
    return todo

