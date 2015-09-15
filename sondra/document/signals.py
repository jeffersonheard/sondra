from blinker import signal

pre_save = signal('document-pre-save')
pre_delete = signal('document-pre-delete')