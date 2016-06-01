#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Dump all replication events from a remote mysql server
#

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)
from pymysqlreplication.event import (
    QueryEvent, RotateEvent, FormatDescriptionEvent,
    XidEvent, GtidEvent, StopEvent,
    BeginLoadQueryEvent, ExecuteLoadQueryEvent,
    NotImplementedEvent
)

MYSQL_SETTINGS = {
    "host": "127.0.0.1",
    "port": 22695,
    "user": "msandbox",
    "passwd": "msandbox"
}


def main():
    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                server_id=3,
                                blocking=True)

    for binlogevent in stream:
        #print binlogevent
        #if isinstance(binlogevent, QueryEvent):
        #    print binlogevent.query
        if isinstance(binlogevent, WriteRowsEvent):
            for rows in binlogevent.rows:
                print rows
                #print binlogevent.query
        #binlogevent.dump()

    stream.close()


if __name__ == "__main__":
    main()
