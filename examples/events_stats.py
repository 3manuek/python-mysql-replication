#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Dump all replication events from a remote mysql server
# and generates statistics per given interval.
#
# Useful for a logical stream metrics for debugging application
# writes causing unnecesary writes or affecting replication.
#
# Currently only compatible with binlog_format = ROW
# For statement, instead iterate over rows, we should do:
#
#    for binlogevent in stream:
#        if isinstance(binlogevent, QueryEvent):
#            print binlogevent.query

import re, sys
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
#from datetime import (timedelta, datetime)
import time
from pudb import set_trace

#from collections import defaultDict()

MYSQL_SETTINGS = {
    "host": "127.0.0.1",
    "port": 22695,
    "user": "msandbox",
    "passwd": "msandbox"
}

# CAUTION: The larger the interval, the more memory needed
# for stats generator. Test and put a limit.
# Possible incremental stats would be more efficient, or
# using a database in memory manageable datastore.

OPTIONS = {
    "interval": 10, # in seconds
    "serverid": 3,
    "log_file": "mysql-bin.000002",
    "log_pos": 4
    #"pattern": "http?:\/\/(.*)\s?" # data pattern to search
}


# find a way to avoid occurrence.strip('/:')

def calculateStats():
    # Top N operation or top 30%/20% ops
    # Estimate variance
    # Last 5, 10 , 15 "load average style"
    countOps = len(opsGeneralCollector)
    countPatterns = len(patternGeneralCollector)

    patternGeneralCollector = sorted(patternGeneralCollector, reverse = True)
    opsGeneralCollector = sorted(opsGeneralCollector, reverse = True)


def printStats():
    # Print Top Operations
    countTopN = 0
    topN = 10
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

    print "Total ops: %s , Ops collected: %s, Pattern occurrences: %s " % (totalOps,countOps,countPatterns)


def main():
    global patternGeneralCollector
    global opsGeneralCollector
    global totalOps # Reset on each interval
    global countOps
    global countPatterns
    global patternC
    #global generalCollector
    patternGeneralCollector = {}
    opsGeneralCollector = {}
    #generalCollector = {}
    totalOps = 0 # Reset on each interval
    countOps = 0
    countPatterns = 0
    prevTimeFlag = 0

    # Great resource http://blog.mattheworiordan.com/post/13174566389/url-regular-expression-for-links-with-or-without
    patternC = re.compile(r'http?:\/\/(?:[A-Za-z0-9\.\-]+)')

    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    #timeCheck = datetime.today()  # If event is bigger, sets it to now() and prints stats.
    timeCheck = time.time()
    print "Starting at %s " % (timeCheck)
    # We always want to use MASTER_AUTO_POSITION = 1
    # Only events help us to keep the stream shorter as we can.
    stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                server_id=OPTIONS["serverid"],
                                log_file=OPTIONS["log_file"],
                                #auto_position=1,
                                blocking=True,
                                only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent])

    for binlogevent in stream:
        patternCollector = None
        occurrence = None
        if prevTimeFlag == 0:
            txTimeCheck = binlogevent.timestamp
            prevTimeFlag = 1

        for row in binlogevent.rows:
            if isinstance(binlogevent, DeleteRowsEvent):
                vals = row["values"]
                eventType = "delete"
            elif isinstance(binlogevent, UpdateRowsEvent):
                vals = row["after_values"]
                eventType = "update"
            elif isinstance(binlogevent, WriteRowsEvent):
                vals = row["values"]
                eventType = "insert"

            occurrence = re.search(patternC, str(vals) )
            if  occurrence:
                occurrence_ = (occurrence.group()).strip('http?://')
                patternCollector = "%s__%s__%s_%s" % (
                                        eventType,
                                        occurrence_, #.group(),
                                        binlogevent.schema, binlogevent.table,
                                        )

            tableCollector = "%s__g__%s_%s" % (
                                    eventType,
                                    binlogevent.schema, binlogevent.table,
                                    )

        if patternCollector:
            if patternCollector in patternGeneralCollector.keys():
                patternGeneralCollector[patternCollector] += 1
            else:
                patternGeneralCollector[patternCollector] = 1

        if tableCollector in opsGeneralCollector.keys():
            opsGeneralCollector[tableCollector] += 1
            #countOps += 1
        else:
            opsGeneralCollector[tableCollector] = 1
            #countOps += 1

        totalOps += 1

        # If interval has been committed, print stats and reset everything
        if (timeCheck + OPTIONS["interval"]) < time.time() or txTimeCheck + OPTIONS["interval"] > time.time():
            #print "Entering line stats at %s " % (timeCheck)
            #calculateStats()
            printStats()
            ## Reset everything to release memory
            totalOps = 0
            patternGeneralCollector = {}
            generalCollector = {}
            timeCheck = time.time()
            #txTimeCheck = binlogevent.timestamp
            prevTimeFlag = 0

    stream.close()


if __name__ == "__main__":
    main()
