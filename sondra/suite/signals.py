from blinker import signal

pre_init = signal('suite-pre-init')
post_init = signal('suite-post-init')
pre_app_registration = signal('suite-pre-app-registration')
post_app_registration = signal('suite-post-app-registration')