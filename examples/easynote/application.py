from sondra.auth import AuthorizableApplication
from sondra.lazy import fk
from .collections import Tags, Notebooks, Notes

class EasyNote(AuthorizableApplication):
    """
    This application contains all the main data for EasyNote.
    """

    collections = (
        Tags,
        Notebooks,
        Notes,
    )

    definitions = {
        "log_entry": {
            "type": "object",
            "properties": {
                "kind": {"enum": [
                    "content changed",
                    "attachment changed",
                    "tag changed",
                    "sharing changed",
                    "notebook changed",
                ]},
                "author": fk('api/auth/users'),
                "timestamp": {"type": "string", "format": "datetime"}
            }
        },
        "content_changed": {
            "type": "object",
            "allOf": ["#/definitions/log_entry"],
            "properties": {
                "diff": {"type": "string"}
            }
        },
        "attachment_changed": {
            "type": "object",
            "allOf": ["#/definitions/log_entry"],
            "properties": {
                "change": {"enum": ["added", "deleted"]},
                "filename": {"type": "string"}
            }
        },
        "tag_changed": {
            "type": "object",
            "allOf": ["#/definitions/log_entry"],
            "properties": {
                "change": {"enum": ["added", "deleted"]},
                "tag": {"type": "string"}
            }
        },
        "sharing_changed": {
            "type": "object",
            "allOf": ["#/definitions/log_entry"],
            "properties": {
                "change": {"enum": ["added read", "deleted read", "added write", "deleted write"]},
                "with": fk('api/auth/users')
            }
        },
        "notebook_changed": {
            "type": "object",
            "allOf": ["#/definitions/log_entry"],
            "properties": {
                "from": fk('api/auth/notebook'),
                "to": fk('api/auth/notebook')
            }
        }
    }

