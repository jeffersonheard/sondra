import click
import io
from docutils.core import publish_string
import importlib
import os.path
from sondra.help import SchemaHelpBuilder
import logging
from sphinxcontrib import napoleon

logging.basicConfig(level=logging.DEBUG)

@click.group()
def cli():
    pass

@cli.command()
@click.option("--format", '-f', type=click.Choice(['html','rst', 'odt']), default='html')
@click.option("--destpath", '-d', default='.')
@click.argument("classnames", nargs=-1)
def classes(format, destpath, classnames):
    for c in classnames:
        output_filename = os.path.join(destpath, c + '.' + format)
        module_name, classname = c.rsplit('.', 1)
        mod = importlib.import_module(module_name)
        klass = getattr(mod, classname)
    try:
        with open(output_filename, 'w') as output:
            tmp = io.StringIO()
            tmp.write("#" * len(klass.__name__))
            tmp.write("\n")
            tmp.write(klass.__name__)
            tmp.write('\n')
            tmp.write("#" * len(klass.__name__))
            tmp.write("\n\n")

            tmp.write(str(napoleon.GoogleDocstring(klass.__doc__)))
            tmp.write('\n\n')

            builder = SchemaHelpBuilder(klass.schema, fmt=format)
            tmp.write(builder.rst)

            if format == 'html':
                output.write(publish_string(tmp.getvalue(), writer_name='html', settings_overrides={"stylesheet_path": "sondra/css/flasky.css"}).decode('utf-8'))
            elif format == 'rst':
                output.write(tmp.getvalue())
        logging.info("Wrote {0} to {1}".format(c, output_filename))
    except Exception as e:
        logging.error(str(e))


@cli.command()
@click.option("--format", '-f', type=click.Choice(['html','rst', 'odt']), default='html')
@click.option("--destpath", '-d', default='.')
@click.argument("suite", nargs=1)
@click.argument("apps", nargs=-1)
def suite(format, destpath, suite, apps):
    module_name, classname = suite.rsplit('.', 1)
    mod = importlib.import_module(module_name)
    klass = getattr(mod, classname)
    suite = klass()

    for c in apps:
        module_name, classname = c.rsplit('.', 1)
        mod = importlib.import_module(module_name)
        klass = getattr(mod, classname)
        klass(suite)

    output_filename = os.path.join(destpath, "{suite}.{fmt}".format(suite=suite.name, fmt=format))
    with open(output_filename, 'w') as suite_help:
        suite_help.write(suite.help())

    for app in suite:
        output_filename = os.path.join(destpath, "{suite}.{app}.{fmt}".format(suite=suite.name, app=app, fmt=format))

        try:
            with open(output_filename, 'w') as output:
                tmp = suite[app].help()
                if format == 'html':
                    output.write(suite.docstring_processor(tmp).decode('utf-8'))
                elif format == 'rst':
                    output.write(tmp.getvalue().decode('utf-8'))
            logging.info("Wrote {0} to {1}".format(app, output_filename))
        except Exception as e:
            logging.error(str(e))

        for collection in suite[app]:
            output_filename = os.path.join(destpath, "{suite}.{app}.{coll}.{fmt}".format(suite=suite.name, app=app, coll=collection, fmt=format))

            try:
                with open(output_filename, 'w') as output:
                    tmp = suite[app][collection].help()
                    if format == 'html':
                        output.write(suite.docstring_processor(tmp).decode('utf-8'))
                    elif format == 'rst':
                        output.write(tmp.getvalue().decode('utf-8'))
                logging.info("Wrote {0}.{1} to {2}".format(app, collection, output_filename))
            except Exception as e:
                logging.error(collection)
                logging.error(str(e))

            output_filename = os.path.join(destpath, "{suite}.{app}.{coll}.doc.{fmt}".format(suite=suite.name, app=app, coll=collection, fmt=format))


            try:
                with open(output_filename, 'w') as output:
                    tmp = suite[app][collection].doc({}).help()
                    if format == 'html':
                        output.write(suite.docstring_processor(tmp).decode('utf-8'))
                    elif format == 'rst':
                        output.write(tmp.getvalue().decode('utf-8'))
                logging.info("Wrote {0}.{1} document class to {2}".format(app, collection, output_filename))
            except Exception as e:
                logging.error(collection + " docclass")
                logging.error(str(e))

if __name__=='__main__':
    cli()