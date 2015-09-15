from blinker import signal

pre_init = signal('suite-pre-init')
post_init = signal('suite-post-init')