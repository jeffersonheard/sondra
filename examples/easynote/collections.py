from sondra.auth import AuthorizableCollection
from sondra.collection import DateTime, Geometry, Now

from .documents import Note, Tag, Notebook


class Notebooks(AuthorizableCollection):
    document_class = Notebook
    indexes = (
        'owner',
    )


class Notes(AuthorizableCollection):
    document_class = Note
    specials = {
        'location': Geometry(),
        'creation_timestamp': DateTime(),
    }
    indexes = (
        'title',
        'creation_timestamp',
        'location',
        'author',
        'notebook'
    )


class Tags(AuthorizableCollection):
    document_class = Tag
    primary_key = "tag"
    indexes = ('parent',)