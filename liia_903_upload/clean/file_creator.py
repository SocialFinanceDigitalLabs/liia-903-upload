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
    Save the data events as csv files
    """
    for event in stream:
        if isinstance(event, TableEvent):
            dataset = event.data
            file = dataset.export("csv")
            with open(f"{event.filename[:-4]}_clean.csv", "r") as f:
                f.write(file.csv)

            # f"{event.file_name[:-4]}_clean.csv"


@functools.cache
def lookup_column_config(table_name, column_name):
    return None
