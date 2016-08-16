from sondra.schema import S, deep_merge

schema1 = S.object(
    title="Ticket",
    description='A work order for Pronto',
    required=['title', 'creator', 'status', 'open', 'price'],
    properties=S.props(
        ("asset", S.fk('api','core','assets', title="Asset")),
        ("location", S.geo(description="A copy of asset location, for efficient indexing purposes.", geometry_type='Point')),
        ("title", S.string(title="Title", description="A description of the title")),
        ("ticket_type", S.fk('api','core','ticket-types', title="Ticket Type")),
        ("narrative", S.string(title="Narrative", description="Details relevant to fixing the problem.")),
        ("confirm_before_dispatch", S.boolean(title="Confirm before dispatch", description="True if 365 pronto should confirm with the asset contact before a worker arrives on site", default=False)),
        ("clock_running", S.boolean(default=False)),
        ("next_response_due", S.datetime()),
        ("inconsistencies", S.integer(default=0, description="The number of inconsistencies reported in answers or status changes.")),
        ("flags", S.integer(default=0, description="A count of out of bounds values reported in worksheets.")),
        ("requires_review", S.boolean(default=False)),
        ("designated_reviewer", S.fk('api','auth','users')),
        ("related", S.array(items=S.string(), description='Any tickets whose body of work relates to the completion of this ticket.')),
        ("predecessor", S.string(description='The ticket this ticket was raised as a consequence of.')),
        ("antecedent", S.string(description='The ticket raised as a consequence of this one.')),
        ("required_professionals", S.integer(description="The number of people required on this ticket", default=1)),
        ("assigned_professionals", S.array(items=S.ref('assignee'))),
        ("creator", S.fk('api','auth','users', description="The person who created the ticket")),
        ("assignee", S.fk('api','auth','users', description="The person who currently is responsible for the ticket")),
        ("status", S.ref('ticket_status')),
        ("tech_support_token", S.string(
            description="Automatically generated. Send this token as part of a URL in email to allow a third party "
                        "tech support access to view this ticket and communicate with the assigned professionals "
                        "through the admin console or third-party app."
        )),
        ("open", S.boolean(default=False)),
        ("price", S.string()),
        ("currency", S.string(default='USD')),
        ("customer_billed", S.datetime()),
        ("customer_paid", S.datetime()),
        ("customer_paid_in_full", S.boolean(default=True)),
        ("contractor_paid", S.datetime()),
        ("work_requirements", S.array(items=S.ref('work_requirement'))),
        ("union", S.boolean(description="True if the asset requires a union contractor.")),
        ("prevailing_wage", S.boolean(description="True if the asset requires prevailing wage.")),
        ("worksheets", S.array(items=S.ref('worksheets_for_status'))),
        ("work_performed", S.array(items=S.ref('work'))),
        ('arbitration_required', S.boolean(default=False)),
        ('arbitration_complete', S.boolean()),
        ('result_of_arbitration', S.textarea()),
        ('linked', S.fk('core', 'tickets', description="This is set if this ticket is listed in another ticket's links")),
        ('linked_tickets', S.fk_array('core', 'tickets',
            description="Other tickets that must be complete for this one to be considered finished. These are "
                        "often tickets that are co-located on the same site."))
    ))


schema2 = S.object(
    title="Ticket",
    description='A work order for Pronto',
    required=['owner', 'creator', 'created', 'updated'],
    properties=S.props(
        ("created", S.datetime()),
        ("updated", S.datetime()),
        ("owner", S.fk('api','core','customers', description="Customer who can administer this ticket")),
        ("creator", S.fk('api','auth','users', description="The person who created the ticket")),
    ))

from pprint import PrettyPrinter
pp = PrettyPrinter(indent=4)
merged_schema = (deep_merge(schema1, schema2, 'set'))
pp.pprint(sorted(merged_schema['properties'].keys()))
pp.pprint(sorted(merged_schema['required']))
a = set(merged_schema['properties'].keys())
b = set(schema1['properties'].keys())
c = set(schema2['properties'].keys())

print(b.union(c).difference(a))
