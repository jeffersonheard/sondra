# Sondra 

**Author**: Jefferson Heard
**Copyright**: 2015 Jefferson Heard
**License**: [Apache 2.0](./LICENSE)

Sondra is an "ORM" for Python 3.x, Flask, and RethinkDB with some quite unique features. Sondra's goal is to aid full stack developers by letting them focus on data models and functionality instead of writing workarounds and glue code. It embraces common "shortcuts" developers take in common full-stack web applications, e.g. merging "Model" and "Controller" in the oft-used MVC pattern.

Sondra is not *quite* yet ready for prime time, as it does not yet contain an authentication or authorization mechanism for web APIs. This is the next thing on the list.

Sondra does not currently support asynchronous access to RethinkDB.  The goal is to eventually support [Tornado](http://www.tornadoweb.org/en/stable/)

### Features

* A clear, DRY heirarchical application structure that emphasizes convention over configuration.
* [JSON-Schema](http://json-schema.org/) validation for documents.
* Expose methods on documents, collections, and applications.
* A clear, predictable URL scheme for all manner of API calls, covering a broad set of use-cases.
* Self documenting APIs with both human-readable help based on docstrings and schemas for every call.
* Use API idiomatically over HTTP and native Python without writing boilerplate code

### Concept

A Sondra API is exposed in Flask as a suite of applications.  Each application contains a number of document collections, and each collection contains a number of documents. 

Sondra tries to take advantage of all manner of Python idioms in a sane manner.  It generates as much as possible, while avoiding "magic tricks" or introducing conventions that are already covered by Python idioms. This means that:

* Online documentation is generated at every level from reStructuredText or Google style docstrings. 
* Method schemas are generated from annotated function signatures
* All URL features that ``urllib.parse`` recognizes are taken advantage of to create a regular URL scheme that encompasses all manner of calls.

#### Documents

A ``Document`` is a single document that conforms to a JSON-Schema "object" type. That is, it is never a simple type nor an array of items.

Documents may expose methods to the HTTP api.  These are similar to instance methods in Python.  They operate on an individual document in a collection instead. Document methods might include operations that combine multiple documents to make a third (add, multiply, divide, subtract, or similar) or they might provide specific views of a document.  Anything that you would write as an "instance method" in Python.

#### Collections

A ``Collection`` is a RethinkDB document collection that contains a specific subclass of ``Document``, which is defined by a single JSON-Schema. The collection class defines additionally:

* The primary key name (defaults to the RethinkDB default of "id")
* Indexes
* Any document properties that require "special treatment" in RethinkDB such as geographical and date/time types.
* Relations to other Collections
* The ``Application`` class it belongs to.

Collections may expose methods to the HTTP api.  These are similar to class methods in Python, as they operate on the collection itself and not the individual documents. Collection methods might provide special filtering, create documents according to a specific template, or set properties on the collection itself. Anything you would write as a "class method" in Python

#### Applications

An ``Application`` is a reusable grouping of ``Collection``s and a set of optional *application methods*, which operate a bit like globally available functions. Applications are bound to a single database within RethinkDB.

Applications may expose methods to the HTTP api.  These are similar to the functions that are defined at the module level in Python.  They are not specific to a particular class or instance, but instead are defined to provide broad functionality for the whole application. 

#### The Suite

A ``Suite`` defines the environment of applications, including database connections and provides some basic functionality. Every application is registered with the global ``Suite`` object, which itself implements Python's Mapping protocol to provide dictionary-like lookup of application objects.  The "Suite" object determines the base path of all Application APIs. Suites are similar in nature to Django's ``settings.py`` except that they are class-based. There may be only *one* concrete class of Suite in your Flask app, although it may derive from any number of abstract Suite mixins.



