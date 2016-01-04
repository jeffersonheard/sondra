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
       'sondra.format',
       'sondra.tests',
       'sondra.tests.web',
    ],
    version='0.5.0',
    description='JSON-Schema-based ORM for RethinkDB',
    author="Jefferson Heard",
    author_email="jefferson.r.heard@gmail.com",
    url="https://github.com/JeffHeard/sondra",
    keywords=["json","rethinkdb","flask"],
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Development Status :: 3 - Alpha",
        "Environment :: Web Environment",
        "Framework :: Flask",
        "Framework :: Django",
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
