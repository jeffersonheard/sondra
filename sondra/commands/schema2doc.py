import click
import requests
from urllib.parse import urlparse
import json
import os.path
from sondra.help import SchemaHelpBuilder
import logging

logging.basicConfig(level=logging.DEBUG)

@click.group()
def cli():
    pass


@cli.command()
@click.option("--format", '-f', type=click.Choice(['html','rst', 'odt']), default='html')
@click.option("--destpath", '-d', default='.')
@click.argument("filenames", nargs=-1)
def files(format, destpath, filenames):
    for f in filenames:
        filename, ext = os.path.splitext(os.path.basename(f))
        output_filename = os.path.join(destpath, filename + '.' + format)
    #try:
        with open(f) as input, open(output_filename, 'w') as output:
            builder = SchemaHelpBuilder(json.load(input), fmt=format)
            if format == 'html':
                output.write(builder.html)
            elif format == 'rst':
                output.write(builder.rst)
        logging.info("Wrote {0} to {1}".format(f, output_filename))
    #except Exception as e:
    #    logging.error(str(e))

@cli.command()
@click.option("--format", '-f', type=click.Choice(['html','rst']), default='html')
@click.option("--destpath", '-d', default='.')
@click.argument("urls", nargs=-1)
def urls(format, destpath, urls):
    for url in urls:
        p_url = urlparse(url)
        filename = p_url.path.split('/')[-1]
        if '.' in filename:
            filename, ext = os.path.splitext(filename)
        output_filename = os.path.join(destpath, filename + '.' + format)
        resource = requests.get(url)
        if resource.ok:
        #try:
            with open(output_filename, 'w') as output:
                builder = SchemaHelpBuilder(json.load(input), fmt=format)
                if format == 'html':
                    output.write(builder.html)
                elif format == 'rst':
                    output.write(builder.rst)
                elif format == 'odt':
                    output.write(builder.odt)
            logging.info('Wrote {0} to {1}'.format(url, output_filename))
        #except Exception as e:
        #    logging.error(str(e))


if __name__=='__main__':
    cli()