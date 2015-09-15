from blinker import signal


pre_init = signal('collection-pre-init')
post_init = signal('collection-post-init')
pre_validation = signal('collection-pre-validation')
post_validation = signal('collection-post-validation')
pre_table_creation = signal('collection-pre-table-create')
post_table_creation = signal('collection-post-table-create')
pre_table_deletion = signal('collection-pre-table-deletion')
post_table_deletion = signal('collection-post-table-deletion')
before_validation = signal('collection-before-validation')
after_validation = signal('collection-after-validation')

