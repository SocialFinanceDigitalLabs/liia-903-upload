from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

from liia_903_upload.clean.converters import to_date, to_category


@streamfilter(check=type_check(events.StartElement), fail_function=pass_event, error_function=pass_event)
def clean_dates(event):
    """
    Convert all values that should be dates to dates based on the config.yaml file
    """
    date = event.config
    text = to_date(event.text, date)
    return event.from_event(event, text=text)


@streamfilter(check=type_check(events.StartElement), fail_function=pass_event, error_function=pass_event)
def clean_categories(event):
    """
    Convert all values that should be categories to categories based on the config.yaml file
    """
    category = event.config
    text = to_category(event.text, category)
    return event.from_event(event, text=text)


def clean(stream):
    """
    Compile the cleaning functions
    """
    stream = clean_dates(stream)
    stream = clean_categories(stream)
    return stream
