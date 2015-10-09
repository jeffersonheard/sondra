from docutils.core import publish_string, publish_parts
import io
import logging
import json
import jsonschema


class RSTBuilder(object):
    """A simple builder for reStructuredText help.

    Does not support all of RST by a long shot. However, it provides sufficient functionality to be useful for
    generating help from JSON.
    """
    _log = logging.getLogger('RSTBuilder')
    HEADING_CHARS = """#=*+-~^`_:.'"""

    def __init__(self, fmt='rst', out=None, initial_heading_level=0):
        self._out = out or io.StringIO()
        self._cached_html = None
        self._cached_text = None
        self._cached_odf = None
        self._output = fmt
        self._indents = []
        self._list = []  # a list of "*" or number. Each is a list level.
        self._heading_level = initial_heading_level
        self._indent_str = ''
        self.indent_amt = 2
        self._lines_written = 0

    def __str__(self):
        if self._output == 'rst':
            return self.rst
        else:
            return self.html

    def build(self):
        self._log.debug("Build begun")

    @property
    def odt(self):
        if not self._cached_odf:
            self._log.debug("Creating ODT")
            self._cached_odf = publish_string(self.rst, writer_name='odf_odt').decode('utf-8')
        return self._cached_odf

    @property
    def html(self):
        if not self._cached_html:
            self._log.debug("Creating HTML")
            self._cached_html = publish_parts(self.rst, writer_name='html')['html_body']
        return self._cached_html

    @property
    def rst(self):
        self._cached_text = self._out.getvalue()
        if not self._cached_text:
            self._log.debug("Creating reStructuredText")
            self.build()
            self._log.debug("Finished creating reStructuredText.")
            self._out.flush()
            self._cached_text = self._out.getvalue()
        return self._cached_text

    def indent(self, amt=None):
        amt = amt or self.indent_amt
        self._indents.append(amt)
        ct = sum(self._indents)
        self._indent_str = ' ' * ct

    def dedent(self):
        self._indents.pop()
        ct = sum(self._indents)
        self._indent_str = ' ' * ct

    def full_stop(self):
        self._list = []
        self._heading_level = 0
        self._indents = []
        self._indent_str = ''

    def sep(self, separator=' '):
        self._out.write(separator)

    def begin_subheading(self, title):
        self.end_lists()
        self.line(title)
        self.line(self.HEADING_CHARS[self._heading_level] * len(title))
        self.line()
        self._heading_level += 1

    def end_subheading(self):
        self._heading_level -= 1
        if self._heading_level < 0:
            self._heading_level = 0
        if self._lines_written:
            self.line()
        if self._heading_level == 0:
            self.line()
        self._lines_written = 0

    def begin_code(self, preface=''):
        self.end_lists()
        self.line(preface+'::')
        self.line()
        self.indent(4)

    def end_code(self):
        self._indents = []
        self.line()

    def begin(self, title):
        if len(self._list) > 0:
            self.line(title)
            self.begin_list()
        else:
            self.begin_subheading(title)

    def end(self):
        if len(self._list) > 0:
            self.end_list()
        else:
            self.end_subheading()

    def end_lists(self):
        self._list = []
        self._indents = []

    # def _begin_line(self):
    #     if len(self._list):
    #         self._out.write(self._indent_str)
    #         self._out.write(self._list[-1])
    #         self._out.write(" ")
    #         self._indent(2)
    #     else:
    #         self._out.write(self._indent_str)
    #
    # def _write(self, s):
    #     self._out.write(s)
    #
    # def _end_line(self):
    #     self._out.write('\n')

    def line(self, s=''):
        s = [l.strip() for l in s.splitlines()]
        first, *rest = s if s else (None, [])
        if first:
            if len(self._list):
                self._out.write(self._indent_str)
                self._out.write(self._list[-1])
                self._out.write(" ")
                self.indent(2)
                self._out.write(first)
                self._out.write('\n')
            else:
                self._out.write(self._indent_str)
                self._out.write(first)
                self._out.write('\n')
            for l in rest:
                self._out.write(self._indent_str)
                self._out.write(l)
                self._out.write('\n')
            if self._list:
                self.dedent()
        else:
            self._out.write('\n')

        self._lines_written += 1

    def define(self, term, definition):
        self.line("**{term}** - {definition}".format(term=term, definition=definition))

    def begin_list(self):
        if len(self._list):
            self.indent()

        self.line()

        char = len(self._list) % 3
        markers = "*-+"
        self._list.append(markers[char])

    def end_list(self):
        self._list.pop()
        if len(self._list):
            self.dedent()
        elif self._lines_written:
            self.line()
            self._lines_written = 0



class SchemaHelpBuilder(RSTBuilder):
    """Builds help from a JSON-Schema as either HTML or reStructuredText source."""
    RESERVED_WORDS = {
        "type", "title", "description", "name", "id",
        "properties", "patternProperties", "additionalProperties", "maxProperties", "minProperties",
        "required", "dependencies",
    }

    _log = logging.getLogger("SchemaHelpBuilder")

    def __init__(self, schema, url="", fmt='rst', out=None, initial_heading_level=0):
        """Builds help from a JSON-Schema as either HTML or reStructuredText source.

        Args:
            schema: a json-schema as a python dict.
            output (str): either `"html"` or `"rst"`
        """
        super(SchemaHelpBuilder, self).__init__(fmt=fmt, out=out, initial_heading_level=initial_heading_level)

        # validate a schema before we write help for it.
        jsonschema.Draft4Validator.check_schema(schema)

        self.schema = schema
        self.url = url or schema.get('id', url)
        self._refs = {}

    def _make_definition(self, name, subschema):
        if "id" in subschema:
            ref_name = "#" + subschema['id']
        else:
            ref_name = "#/definitions/{name}".format(name=name)

        self._refs[ref_name] = subschema
        return ref_name

    def _gather_definitions(self):
        for name, subschema in self.schema.get('definitions', {}).items():
           ref_name = self._make_definition(name, subschema)
           self._log.debug("Found {0} in definitions, caching as {1}.".format(name, ref_name))

    def build(self):
        super(SchemaHelpBuilder, self).build()

        self._gather_definitions()

        self._write_fragment(self.schema)

        if "definitions" in self.schema:
            self.begin_subheading("Definitions")
            for name, ref in self.schema["definitions"].items():
                self._write_fragment(ref, name=name)
            self.end_subheading()

        self._log.debug("Build finished.")

    def _write_link_or_fragment(self, frag, name=None, title=None):
        if "$ref" in frag:
            self.line('`{link}`_'.format(link=self._link_name(frag["$ref"])))
        else:
            self._write_fragment(frag, name=name, title=title)

    def _write_fragment(self, frag, name=None, title=None):
        fragment_type = self._type_specifier(frag)
        assigned_title = title is not None
        if not title:
            # set the title of the fragment
            if name:
                title = "{name}".format(name=name)
            elif 'title' in frag:
                title = frag['title']

        if title:
            self.begin(title)

        if 'name' in frag:
            self.define('Name', frag['name'])
            self.line()

        if 'id' in frag:
            self.define('Id', "``" + frag['id'] + "``")
            self.line()

        if not assigned_title:
            if "type" in frag:
                if 'refersTo' in frag:
                   self.define("Refers To", frag['refersTo'])
                else:
                    self.define('Type', frag['type'])
                self.line()
            if "description" in frag:
                self.define("Description", frag['description'])
            if "default" in frag:
                self.define("Default", frag['default'])
            self.line()

        self._validation_specifiers(frag)

        if fragment_type == 'object':
            self._write_object(frag)
        elif fragment_type == 'array':
            self._write_array(frag)
        elif fragment_type == 'string':
            self._write_string(frag)
        elif fragment_type == 'integer':
            self._write_numeric(frag)
        elif fragment_type == 'float':
            self._write_numeric(frag)
        else:
            self._write_object(frag)

        if title:
            self.end()

    def _write_property(self, name, typedef):
        title = "**{name}** {type_specifier}{default} - {description}".format(
            name=name,
            default=(" = ``{0}``".format(typedef['default'])) if 'default' in typedef else '',
            type_specifier=self._type_specifier(typedef),
            description=self._description(typedef)
        )
        self._write_fragment(typedef, name=name, title=title)

    def _write_property_list(self, props):
        self.begin_list()
        for prop, defn in sorted(props.items(), key=lambda x: x[0]):
            self._write_property(prop, defn)
        self.end_list()

    def _write_object(self, typedef):
        self.begin_list()
        if "minProperties" in typedef:
            self.define("Min Properties", typedef['minProperties'])

        if "maxProperties" in typedef:
            self.define("Max Properties", typedef['maxProperties'])

        if "required" in typedef:
            self.define("Required", ", ".join(typedef['required']))
        self.end_list()

        if "dependencies" in typedef:
            self.begin('Dependencies')
            for name, deps in typedef['dependencies'].items():
                deps = typedef['dependencies']
                if isinstance(deps, list):
                    self.define("Property dependency on **{0}**".format(name), ', '.join(deps))
                else:
                    self.begin("Schema dependency on {0}".format(name))
                    self._write_fragment(deps)
                    self.end()
            self.end()

        if "properties" in typedef:
            self.begin("Properties")
            self._write_property_list(typedef['properties'])
            self.end()

        if "additionalProperties" in typedef:
            if isinstance(typedef['additionalProperties'], bool):
                self.define("Additional properties", typedef['additionalProperties'])
            else:
                self.begin("Additional Properties:")
                self._write_property_list(typedef['additionalProperties'])
                self.end()

        if "patternProperties" in typedef:
            self.begin("Pattern Properties")
            self._write_property_list(typedef['patternProperties'])
            self.end()

    def _write_array(self, typedef):
        if "items" in typedef:
            if isinstance(typedef['items'], list):
                self.begin_list()
                for x in typedef['items']:
                    self._write_link_or_fragment(x)
                self.end_list()
            else:
                self.begin("**Items**")
                self._write_link_or_fragment(typedef['items'])
                self.end()

        if "additionalItems" in typedef:
            if isinstance(typedef['additionalItems'], bool) and typedef['additionalItems']:
                self.line("**Items are limited to the list above_**")
            elif isinstance(typedef['additionalItems'], list):
                self.begin_list()
                for x in typedef['additionalItems']:
                    self._write_link_or_fragment(x)
                self.end_list()
            else:
                self._write_link_or_fragment(typedef['additionalItems'])

        if "minItems" in typedef:
            self.define("Min items", typedef['minItems'])

        if "maxItems" in typedef:
            self.define("Max items", typedef['maxItems'])

        if typedef.get("uniqueItems", False):
            self.line("**All items must be unique**")

    def _write_numeric(self, typedef):
        if "multipleOf" in typedef:
            self.define("Must be a multiple of", typedef['multipleOf'])

        if "minimum" in typedef:
            self.define("Minimum", typedef['minimum'])

        if "exclusiveMinimum" in typedef:
            self.define("Greater than", typedef['exclusiveMinimum'])

        if "maximum" in typedef:
            self.define("Maximum", typedef['maximum'])

        if "exclusiveMaximum" in typedef:
            self.define("Less than", typedef['exclusiveMaximum'])

    def _write_string(self, typedef):
        if "minLength" in typedef:
            self.define("Min length", typedef['minLength'])

        if "maxLength" in typedef:
            self.define("Max length", typedef['maxLength'])

        if "pattern" in typedef:
            self.define("Matches regular expression", "``" + typedef['pattern'] + "``")

        if "format" in typedef:
            self.define("Required format", typedef['format'])

    def _type_specifier(self, typedef):
        if "type" in typedef:
            if 'refersTo' in typedef:
                return typedef['refersTo']
            else:
                return typedef['type']
        elif "$ref" in typedef:
            r = typedef["$ref"].rsplit('/', 1)[-1]
            if r.startswith('#'):
                r = r[1:]
            return '`' + r + '`_'
        elif "enum" in typedef and len(typedef['enum']):
            value = typedef["enum"][0]
            if isinstance(value, dict):
                return 'object'
            elif isinstance(value, list):
                return 'array'
            elif isinstance(value, int):
                return 'number'
            else:
                return 'string'
        elif "allOf" in typedef:
            return "All:" + ', '.join(self._type_specifier(x) for x in typedef['allOf'])
        elif "anyOf" in typedef:
            return "Any:" + ', '.join(self._type_specifier(x) for x in typedef['anyOf'])
        elif "oneOf" in typedef:
            return "One:" + ', '.join(self._type_specifier(x) for x in typedef['oneOf'])

        return None

    def _validation_specifiers(self, typedef):
        """Todo: write out JSON validation specifiers as a new ul"""
        if "enum" in typedef:
            self.define("Valid values", json.dumps(typedef["enum"]))

        if "allOf" in typedef:
            self.begin("Inherits")
            self.begin_list()
            for frag in typedef['allOf']:
                self._write_link_or_fragment(frag)
            self.end_list()
            self.end()

        if "anyOf" in typedef:
            self.begin("Any of")
            self.begin_list()
            for frag in typedef['anyOf']:
                self._write_link_or_fragment(frag)
            self.end_list()
            self.end()

        if "oneOf" in typedef:
            self.begin("One of")
            self.begin_list()
            for frag in typedef['oneOf']:
                self._write_link_or_fragment(frag)
            self.end_list()
            self.end()

        if "not" in typedef:
            self.begin("Not")
            self._validation_specifiers(typedef["not"])
            self.end()

    def _metadata_keywords(self, typedef):
        if "title" in typedef:
            pass

        if "default" in typedef:
            self.define("Default value", typedef['default'])

        if "description" in typedef:
            pass

    def _link_name(self, reference):
        target = self._refs[reference]
        if "title" in target:
            return target["title"]
        else:
            return reference.rsplit('/', 1)[-1]

    def _deref(self, reference):
        if reference in self._refs:
            link_name = self._link_name(reference)
        else:
            link_name = reference

        return "`{link_name}`_".format(link_name=link_name)

    def _description(self, typedef):
        if "description" in typedef:
            if "title" in typedef:
                return "__{title}__. {description}".format(**typedef)
            else:
                return typedef['description']
        elif "title" in typedef:
            return typedef['title']
        else:
            return "*No description provided.*"

