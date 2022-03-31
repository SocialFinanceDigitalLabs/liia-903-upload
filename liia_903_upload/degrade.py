from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser.events import StartElement
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

from liia_903_upload.converters import to_short_postcode, to_month_only_dob


@streamfilter(check=type_check(StartElement), fail_function=pass_event, error_function=pass_event)
def degrade_postcodes(event):
    """
    Convert all values that should be postcodes to shorter postcodes
    """
    if event.label == "HOME_POST":
        text = to_short_postcode(event.text)
    elif event.label == "PL_POST":
        text = to_short_postcode(event.text)
    else:
        pass
    return event.from_event(event, text=text)


@streamfilter(check=type_check(StartElement), fail_function=pass_event, error_function=pass_event)
def degrade_dob(event):
    """
    Convert all values that should be dates of birth to months and year of birth
    """
    dob = event.config["DOB"]
    text = to_month_only_dob(event.text, dob)
    return event.from_event(event, text=text)


def degrade(stream):
    """
    Compile the degrading functions
    """
    stream = degrade_postcodes(stream)
    stream = degrade_dob(stream)
    return stream
