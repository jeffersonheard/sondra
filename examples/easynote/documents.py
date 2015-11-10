from sondra.decorators import expose
from sondra.document import Document
from sondra.auth import AuthorizableDocument
from sondra.lazy import ref, fk


class Tag(Document):
    """
    A heirarchical index of tags for users to apply to notebooks
    """
    schema = {
        "type": "object",
        "properties": {
            "tag": {"type": "string"},
            "parent": fk('api/easy-note/tags'),
            "description": {"type": "string"},
            "notebooks": {
                "type": "array",
                "items": fk('api/easy-note/notebooks')
            }
        }
    }



class Note(AuthorizableDocument):
    """
    The main class for a note associated with a notebook.
    """
    schema = {
        "type": "object",
        "properties": {
            "title": {
                "type": "string",
                "title": "Title",
                "description": "The note title"
            },
            "content": {
                "type": "string",
                "title": "Content",
                "description": "The content of the note, in markdown format.",
                "format": "markdown"
            },
            "attachments": {
                "type": "array",
                "items": ref('self', "attachment")
            },
            "tags": {
                "type": "array",
                "items": "string"
            },
            "location": ref('api', 'geojsonPoint'),
            "creation_timestamp": {
                "type": "string",
                "title": "Created On",
                "description": "Creation date and time",
                "format": "datetime"
            },
            "author": fk('api/auth/users'),
            "history": {
                "type": "array",
                "items": ref("api/easy-note", "log_entry")
            },
            "notebook": fk('api/easy-note/notebooks')
        }
    }

    definitions = {
        "attachment": {
            "type": "object",
            "properties": {
                "filename": {"type": "string"},
                "description": {"type": "string"},
                "timestamp": {"type": "string", "format": "datetime"}
            }
        }
    }

    @expose
    def update_content(self, content: str) -> None:
        """Update only the content of a note, leaving a history entry"""

    @expose
    def attach_file(self, attachment: bytes, filename: str) -> None:
        """Attach a new file to a note."""

    @expose
    def add_tag(self, tag: str) -> None:
        """Add a tag to a note."""

    @expose
    def remove_tag(self, tag: str) -> None:
        """Remove a tag from a note."""

    @expose
    def remove_file(self, filename: str) -> None:
        """Remove a file attachment."""

    @expose
    def share(self, user: "api/auth/users", read=True, write=True) -> None:
        "Share this note with a user."


class Notebook(AuthorizableDocument):
    """
    The notebook container
    """
    schema = {
        "type": "object",
        "properties": {
           "title": {
                "type": "string",
                "title": "Title",
                "description": "The note title"
            },
            "owner": fk('api/auth/users'),
            "public": {
                "type": "string",
                "enum": {
                    "read",
                    "write",
                    "read-write"
                }
            },
            "shared_with": {
                "type": "array",
                "items": ref('self', 'share')
            }
        }
    }

    definitions = {
        "share": {
            "type": "object",
            "properties": {
                "user": fk('api/auth/users'),
                "read": {"type": "boolean", "default": True},
                "write": {"type": "boolean", "default": True},
            }
        }
    }

    @expose
    def tagged_notes(self, tag: str, include_children:str=True) -> [Note]:
        """Find all notes in this notebook with a particular tag"""
