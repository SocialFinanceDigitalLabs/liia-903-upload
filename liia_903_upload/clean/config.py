from pathlib import Path
from columns import column_names
import yaml

from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event
from sfdata_stream_parser.checks import type_check


@streamfilter(check=type_check(events.StartTable), fail_function=pass_event, error_function=pass_event)
def add_table_name(event):
    """
    Match the loaded table name against one of the 10 903 file names
    """
    for table_name, expected_columns in column_names.items():
        if set(event.headers) == set(expected_columns):
            return event.from_event(event, table_name=table_name)


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


@streamfilter(check=type_check(events.Cell), fail_function=pass_event)
def match_config_to_cell(event, config):
    """
    Match the cell to the config file given the table name and cell header
    the config file should be a set of dictionaries for each table, headers within those tables
    and config rules for those headers
    """
    table_config = config[event.table_name]
    config_dict = table_config[event.header]
    return event.from_event(event, config_dict=config_dict)


def load_config():
    """
    Find and return the config yaml file
    """
    data_dir = Path(r"C:\Users\patrick.troy\OneDrive - Social Finance Ltd\Work\Python\liia 903 upload")
    for yaml_file in data_dir.glob("**/*.yaml"):
        yaml_file = yaml_file
        with open(yaml_file) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
            return config
