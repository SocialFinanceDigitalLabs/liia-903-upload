from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

from liia_903_upload.clean.converters import to_date, to_category, to_integer, check_postcode


@streamfilter(check=type_check(events.Cell), fail_function=pass_event, error_function=pass_event)
def clean_dates(event):
    """
    Convert all values that should be dates to dates based on the config.yaml file
    """
    date = event.config_dict["date"]
    text = to_date(event.cell, date)
    return event.from_event(event, cell=text)


@streamfilter(check=type_check(events.Cell), fail_function=pass_event, error_function=pass_event)
def clean_categories(event):
    """
    Convert all values that should be categories to categories based on the config.yaml file
    """
    category = event.config_dict["category"]
    text = to_category(event.cell, category)
    return event.from_event(event, cell=text)


@streamfilter(check=type_check(events.Cell), fail_function=pass_event, error_function=pass_event)
def clean_integers(event):
    """
    Convert all values that should be integers into integers based on the config.yaml file
    """
    numeric = event.config_dict["numeric"]
    text = to_integer(event.cell, numeric)
    return event.from_event(event, cell=text)


@streamfilter(check=lambda x: x.get('header') in ['HOME_POST', 'PL_POST'], fail_function=pass_event)
def clean_postcodes(event):
    """
    Check that all values that should be postcodes are postcodes
    """
    text = check_postcode(event.cell)
    return event.from_event(event, cell=text)


def clean(stream):
    """
    Compile the cleaning functions
    """
    stream = clean_dates(stream)
    stream = clean_categories(stream)
    stream = clean_integers(stream)
    stream = clean_postcodes(stream)
    return stream
