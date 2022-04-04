import functools

import tablib
from sfdata_stream_parser import events


class RowEvent(events.ParseEvent):
    pass


def coalesce_row(stream):
    """
    Create a list of the cell values for a whole row
    """
    row = None
    for event in stream:
        if isinstance(event, events.StartRow):
            row = []
        elif isinstance(event, events.EndRow):
            yield RowEvent.from_event(event, row=row)
            row = None
        elif row is not None and isinstance(event, events.Cell):
            row.append(event.cell)
        else:
            yield event


def create_tables(stream):
    """
    Append all the rows for a given table_name to create one concatenated dataframe
    """
    data = tablib.Dataset()



@functools.cache
def lookup_column_config(table_name, column_name):
    return None