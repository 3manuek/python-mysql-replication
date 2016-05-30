#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Dump all replication events from a remote mysql server
# and generates statistics per given interval.
#
# Useful for a logical stream metrics for debugging application
# writes causing unnecesary writes or affecting replication.


import re
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from datetime import (timedelta, datetime)

#from collections import defaultDict()

MYSQL_SETTINGS = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "msandbox",
    "passwd": "msandbox"
}

# CAUTION: The larger the interval, the more memory needed
# for stats generator. Test and put a limit.
# Possible incremental stats would be more efficient, or
# using a database in memory manageable datastore.

OPTIONS = {
    "interval": "10", # in seconds
    "serverid": "3"
    #"pattern": "http?:\/\/(.*)\s?" # data pattern to search
}

patternGeneralCollector = {}
opsGeneralCollector = {}
totalOps = 0 # Reset on each interval
countOps = 0
countPatterns = 0
patternC = re.compile(r'http?:\/\/(.*)\s?')
# find a way to avoid occurrence.strip('/:')

def calculateStats():
    # Top N operation or top 30%/20% ops
    # Estimate variance
    # Last 5, 10 , 15 "load average style"
    countOps = count(opsGeneralCollector)
    countPatterns = count(patternGeneralCollector)

    patternGeneralCollector = sorted(patternGeneralCollector, reverse = True)
    opsGeneralCollector = sorted(opsGeneralCollector, reverse = True)


def printStats():
    # Print Top Operations
    calculateStats()

    countTopN = 0
    for key in opsGeneralCollector:
        countTopN += 1
        print(key, opsGeneralCollector[key])
        if countTopN >= topN:
            break

    countTopN = 0
    for key in patternGeneralCollector:
        countTopN += 1
        print(key, patternGeneralCollector[key])
        if countTopN >= topN:
            break

    print "Total ops: %s , Ops collected: %s, Pattern occurrences: %s " %
        (totalOps,countOps,countPatterns)


def main():
    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream

    # We always want to use MASTER_AUTO_POSITION = 1
    # Only events help us to keep the stream shorter as we can.
    try:
        stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                    server_id=OPTIONS.serverid,
                                    blocking=True,
                                    only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent])
    else:
        exit()

    timeCheck = datetime.today()  # If event is bigger, sets it to now() and prints stats.

    for binlogevent in stream:
        # Table schema and table are split with __ instead .?
        table = "%s.%s" % (binlogevent.schema, binlogevent.table)

        for row in binlogevent.rows:
            if isinstance(binlogevent, DeleteRowsEvent):
                vals = row["values"]
                #r.delete(prefix + str(vals["id"]))
            elif isinstance(binlogevent, UpdateRowsEvent):
                vals = row["after_values"]
                #r.hmset(prefix + str(vals["id"]), vals)
            elif isinstance(binlogevent, WriteRowsEvent):
                vals = row["values"]
                #r.hmset(prefix + str(vals["id"]), vals)

            occurrence = search(patternC, str(vals) )
            if  occurrence:
                patternCollector = "%s__%s__%s.%s" % (
                                        binlogevent,
                                        occurrence.group(),
                                        binlogevent.schema, binlogevent.table,
                                        )

            tableCollector = "%s__g__%s.%s" % (
                                    binlogevent,
                                    binlogevent.schema, binlogevent.table,
                                    )

            patternGeneralCollector[patternCollector] += 1
            generalCollector[tableCollector] += 1
            totalOps += 1

        # If interval has been committed, print stats and reset everything
        if (datetime.today() + timedelta(seconds=OPTIONS.interval)) < timeCheck:
            printStats()
            # Reset everything to release memory
            totalOps = 0
            patternGeneralCollector = {}
            generalCollector = {}
            timeCheck = datetime.today()

        # binlogevent.dump()

    stream.close()


if __name__ == "__main__":
    main()
