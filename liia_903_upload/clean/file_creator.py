import functools
import tablib
import re

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


class TableEvent(events.ParseEvent):
    pass


def create_tables(stream):
    """
    Append all the rows for a given table to create one concatenated data event
    """
    data = None
    for event in stream:
        if isinstance(event, events.StartTable):
            data = tablib.Dataset(headers=event.headers + ["LA", "YEAR"])
        elif isinstance(event, events.EndTable):
            yield event
            yield TableEvent.from_event(event, data=data)
            data = None
        elif data is not None and isinstance(event, RowEvent):
            data.append(event.row + [event.la, event.year])
        yield event


def save_tables(stream):
    """
    Save the data events as csv files in the Outputs directory
    """
    for event in stream:
        if isinstance(event, TableEvent):
            dataset = event.data
            csv_data = dataset.export("csv")
            file_dir = re.sub("Inputs", "Outputs", event.filename[:-4]) # Remove the .csv from the filename
            with open(f"{file_dir}_clean.csv", "w", newline="") as f:
                f.write(csv_data)


@functools.cache
def lookup_column_config(table_name, column_name):
    return None
