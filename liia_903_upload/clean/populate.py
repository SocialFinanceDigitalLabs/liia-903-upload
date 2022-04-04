import re

from sfdata_stream_parser import events, checks
from sfdata_stream_parser.filters.generic import streamfilter, pass_event


@streamfilter(check=checks.type_check(events.StartRow), fail_function=pass_event, error_function=pass_event)
def add_year_column(event):
    """
    Searches the filename for the year by finding any four-digit number starting with 20
    """
    file_dir = event.file_name
    year = re.search(r"20\d{2}", file_dir).group(0)
    return event.from_event(event, year=year)


@streamfilter(check=checks.type_check(events.StartRow), fail_function=pass_event, error_function=pass_event)
def add_la_column(event):
    """
    Searches the filename for the local authority by finding the folder name before the 903 folder
    """
    file_dir = event.file_name
    la = re.search(r".*\\(.*)\\903", file_dir).group(1)
    return event.from_event(event, la=la)


@streamfilter()
def inherit_year(stream):
    """
    Return the year associated to a row to identify each row's year
    """
    year = None
    for event in stream:
        if isinstance(event, events.StartRow):
            year = event.year
        elif isinstance(event, events.EndRow):
            year = None
        elif year is not None:
            event = event.from_event(event, year=year)
        yield event


@streamfilter()
def inherit_la(stream):
    """
    Return the local authority name associated to a row to identify each row's local authority
    """
    la = None
    for event in stream:
        if isinstance(event, events.StartRow):
            la = event.la
        elif isinstance(event, events.EndRow):
            la = None
        elif la is not None:
            event = event.from_event(event, la=la)
        yield event


def populate(stream):
    """
    Compile the populate functions
    """
    stream = add_year_column(stream)
    stream = add_la_column(stream)
    stream = inherit_year(stream)
    stream = inherit_la(stream)
    return stream
