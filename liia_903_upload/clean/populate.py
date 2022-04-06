import re

from sfdata_stream_parser import events, checks
from sfdata_stream_parser.filters.generic import streamfilter, pass_event


@streamfilter(check=checks.type_check(events.StartContainer), fail_function=pass_event)
def add_year_column(event):
    """
    Searches the filename for the year by finding any four-digit number starting with 20
    """
    file_dir = event.filename
    match = re.search(r"20\d{2}", file_dir)
    assert match is not None, f"No year found in {file_dir}"
    year = match.group(0)
    return event.from_event(event, year=year)


@streamfilter(check=checks.type_check(events.StartContainer), fail_function=pass_event)
def add_la_column(event):
    """
    Searches the filename for the local authority by finding the folder name before the 903 folder
    """
    file_dir = event.filename
    match = re.search(r".*\\(.*)\\903", file_dir)
    assert match is not None, f"No local authority found in {file_dir}"
    la = match.group(1)
    return event.from_event(event, la=la)


def populate(stream):
    """
    Compile the populate functions
    """
    stream = add_year_column(stream)
    stream = add_la_column(stream)
    return stream
