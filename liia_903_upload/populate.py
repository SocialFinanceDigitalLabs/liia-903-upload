from sfdata_stream_parser import events, checks
from sfdata_stream_parser.filters.generic import streamfilter, pass_event
import re


@streamfilter(check=checks.type_check(events.StartContainer), fail_function=pass_event, error_function=log_error)
def add_year_column(event):
    """
    Searches the filename for the year by finding any four-digit number starting with 20
    """
    file_dir = event.from_event(event, filename=str(event.path.resolve()))
    find_year = re.compile(r"20\d{2}")
    year = find_year.findall(file_dir)
    return year


@streamfilter(check=checks.type_check(events.StartContainer), fail_function=pass_event, error_function=log_error)
def add_la_column(event):
    """
    Searches the filename for the local authority by finding
    """
    file_dir = event.from_event(event, filename=str(event.path.resolve()))
    find_la = re.compile(r"")
    la = find_la.findall(file_dir)
    return la