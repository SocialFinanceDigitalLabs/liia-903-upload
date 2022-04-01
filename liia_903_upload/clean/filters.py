from typing import List

from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser.events import StartElement, EndElement
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

from liia_903_upload.clean.converters import to_date, to_category


@streamfilter(default_args=lambda: {"context": []})
def add_context(event, context: List[str]):
    if isinstance(event, StartElement):
        context.append(event.tag)
        event = StartElement.from_event(event, context=tuple(context))
    elif isinstance(event, EndElement):
        event = EndElement.from_event(event, context=tuple(context))
        context.pop()
    return event


@streamfilter(error_function=pass_event)
def add_config(event, fields):
    path = "/".join(event.context)
    field_config = fields.get(path)
    return event.from_event(event, config=field_config)


@streamfilter(check=type_check(StartElement), fail_function=pass_event, error_function=pass_event)
def clean_dates(event):
    """
    Convert all values that should be dates to dates based on the config.yaml file
    """
    date = event.config['date']
    text = to_date(event.text, date)
    return event.from_event(event, text=text)


@streamfilter(check=type_check(StartElement), fail_function=pass_event, error_function=pass_event)
def clean_categories(event):
    """
    Convert all values that should be categories to categories based on the config.yaml file
    """
    category = event.config['category']
    text = to_category(event.text, category)
    return event.from_event(event, text=text)


def clean(stream):
    """
    Compile the cleaning functions
    """
    stream = add_context(stream)
    stream = add_config(stream)
    stream = clean_dates(stream)
    stream = clean_categories(stream)
    return stream
