from blinker import signal

pre_init = signal('application-pre-init')
post_init = signal('application-post-init')
pre_registration = signal('application-pre-registration')
post_registration = signal('application-post-registration')
pre_create_database = signal('application-pre-create-database')
post_create_database = signal('application-post-create-database')
pre_create_tables = signal('application-pre-create-tables')
post_create_tables = signal('application-post-create-tables')
pre_delete_database = signal('application-pre-delete-database')
post_delete_database = signal('application-post-delete-database')
pre_delete_tables = signal('application-pre-delete-tables')
post_delete_tables = signal('application-post-delete-tables')