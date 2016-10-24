from distutils.core import setup

setup(
    name='sondra',
    packages=[
       'sondra',
       'sondra.auth',
       'sondra.commands',
       'sondra.application',
       'sondra.collection',
       'sondra.document',
       'sondra.suite',
       'sondra.formatters',
       'sondra.tests',
       'sondra.tests.web',
    ],
    version='1.0.0',
    description='JSON-Schema-based ORM for RethinkDB',
    author="Jefferson Heard",
    author_email="jefferson.r.heard@gmail.com",
    url="https://github.com/JeffHeard/sondra",
    keywords=["json","rethinkdb","flask"],
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta",
        "Environment :: Web Environment",
        "Framework :: Flask",
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Software Development :: Libraries",
        "Natural Language :: English",
   ]
)
