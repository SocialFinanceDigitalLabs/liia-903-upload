from sfdata_stream_parser import events, checks
from sfdata_stream_parser.filters.generic import streamfilter, pass_event
import re


@streamfilter(check=checks.type_check(events.StartContainer), fail_function=pass_event, error_function=pass_event)
def add_year_column(event):
    """
    Searches the filename for the year by finding any four-digit number starting with 20
    """
    file_dir = event.from_event(event, filename=str(event.path.resolve()))
    year = re.search(r"20\d{2}", file_dir).group(0)
    return year


@streamfilter(check=checks.type_check(events.StartContainer), fail_function=pass_event, error_function=pass_event)
def add_la_column(event):
    """
    Searches the filename for the local authority by finding the folder name before the 903 folder
    """
    file_dir = event.from_event(event, filename=str(event.path.resolve()))
    la = re.search(r".*\\(.*)\\903", file_dir).group(1)
    return la
