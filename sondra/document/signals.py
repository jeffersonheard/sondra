from blinker import signal

pre_save = signal('document-pre-save')
pre_delete = signal('document-pre-delete')
post_save = signal('document-post-save')
post_delete = signal('document-post-delete')