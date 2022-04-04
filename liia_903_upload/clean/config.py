from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event
from sfdata_stream_parser.checks import type_check
from columns import column_names


def inherit_table_name(stream):
    """
    Return the table name associated to a row to identify each row's table name
    """
    table_name = None
    for event in stream:
        if isinstance(event, events.StartTable):
            table_name = event.table_name
        elif isinstance(event, events.EndTable):
            table_name = None
        elif table_name is not None:
            event = event.from_event(event, table_name=table_name)
        yield event


@streamfilter(check=type_check(events.StartTable), fail_function=pass_event, error_function=pass_event)
def add_table_name(event):
    """
    Match the loaded table name against one of the 10 903 file names
    """
    for table_name, expected_columns in column_names.items():
        if set(event.headers) == set(expected_columns):
            return event.from_event(event, table_name=table_name)


def match_config_to_cell(config):
    column_type = list(config[column].keys())[0]
