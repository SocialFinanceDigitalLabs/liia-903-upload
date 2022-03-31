from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser.events import StartElement
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

from liia_903_upload.converters import to_date, to_category


@streamfilter(check=type_check(StartElement), fail_function=pass_event, error_function=pass_event)
def clean_dates(event):
    date = event.config['date']
    text = to_date(event.text, date)
    return event.from_event(event, text=text)


@streamfilter(check=type_check(StartElement), fail_function=pass_event, error_function=pass_event)
def clean_categories(event):
    category = event.config['category']
    text = to_category(event.text, category)
    return event.from_event(event, text=text)


def clean(stream):
    stream = clean_dates(stream)
    stream = clean_categories(stream)
    return stream
