#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# gen-data.py 
# Demo Pipelines - Generate a historical time set with some changes.
#
# Copyright (c) 2020-2023 Data Culpa, Inc. All rights reserved.
#
# Proprietary and Confidential.  Unauthorized use, copying or dissemination
# of these materials is strictly prohibited.
#

from dataculpa import DataCulpaValidator

from GenWords import ValueGenerator

import numpy as np
import pandas as pd

import argparse
import json
import os
import random
import string
import sqlite3
import sys
#import holidays
import time
import traceback
from datetime import datetime, timedelta, timezone

from multiprocessing import Process

T_VALUES_SCALE = 1<<1
T_VALUES_ALL_STRINGS = 1<<2
T_VALUES_SOME_STRINGS = 1<<3
T_SCHEMA_NAME = 1<<4
T_STRING_LONG_TO_SMALL = 1<<5
T_STRING_SMALL_TO_LONG = 1<<6
T_NULLS_HIGH = 1<<7
T_ZEROS_HIGH = 1<<8

tInt = 0
tFloat = 1
tStrLong = 2
tStrCategory = 3


CATEGORIES = []

D_INCREMENT = 1<<0
D_UNIFORM = 1<<1
D_NORMAL = 1<<2

APPROX_ROWS_PER_DAY = 500
#ROWS_PER_DAY = 100000
#ROWS_PER_DAY = 10000000
NUM_DAYS = 10

class ColumnSpace:
    def __init__(self, fieldName, dataType, t_perc=0.5, t_type=0, dist_flags=0):
        self.d = {}
        self.dataType = dataType
        self.fieldName = fieldName
        self.t_type = t_type
        self.t_perc = t_perc
        self.dist_flags = dist_flags
        self.wordGen = None
        if dataType == tStrLong or dataType == tStrCategory:
            self.wordGen = ValueGenerator()
        self.cache_conn = None
        self.cache_cursor = None

    def _genWord(self, dayIndex):
        if self.dataType == tStrLong:
            if self.hasTransition(dayIndex, T_STRING_LONG_TO_SMALL):
                # one word from our category.
                return self.wordGen.rand_cat_str()

            # up to 20 words I guess...?
            return self.wordGen.rand_words(20)
        elif self.dataType == tStrCategory:
            if self.hasTransition(dayIndex, T_STRING_SMALL_TO_LONG):
                return self.wordGen.rand_words(5)

            return self.wordGen.rand_cat_str()
        else:
            #assert False, "we shouldn't be here: generating words for non-string?"
            # I guess we could be in the number transition value and calling for a word.
            return self.wordGen.rand_cat_str()
        
        assert False, "we shouldn't get here"
        return

    def _convert_type(self, x):
        X = []
        for i in range(len(x)):
            if self.dataType == tInt and (x[i] != 0 and x[i] != ""):
                X.append(int(x[i] * 100))
            else:
                X.append(x[i]) # passthru
        return X

    def _dist_gen(self, numValues):
        if self.hasDist(D_INCREMENT):
            x = np.arange(0, numValues, 1.0)
                
        if self.hasDist(D_UNIFORM): 
            x = np.random.rand(numValues)
        
        if self.hasDist(D_NORMAL): 
            x = np.random.normal(0, 100, numValues)
        
        assert len(x) == numValues, "len(x) = %s !== %s" % (len(x), numValues)
        # FIXME: fancier stuff later: holidays, weekends, trends up and down, etc.
        assert x is not None
        return x
    
    def hasDist(self, mask):
        return (self.dist_flags & mask) != 0

    def hasTransition(self, day_index, mask, print_debug=False):
        t_day = int(NUM_DAYS * self.t_perc)
        #print(t_day, day_index)
        if day_index < t_day:
            return False
        return (self.t_type & mask) != 0

    def get_field_name(self, day_index):
        if self.hasTransition(day_index, T_SCHEMA_NAME):
            return "new-" + self.fieldName
        return self.fieldName

    def replace_half(self, x, new_value):
        x = list(x)
        for i in range(len(x)):
            if random.random() > 0.5:
                x[i] = new_value
        return x

    def get_cache_for(self, dayIndex):
        p = ".%s-%d.cache" % (self.fieldName, dayIndex)
        return p

    def open_sqlite_cache(self, dayIndex):
        assert self.cache_cursor is None
        assert self.cache_conn is None

        p = self.get_cache_for(dayIndex)
        if os.path.exists(p):
            os.unlink(p)
        conn = sqlite3.connect(p)
        cursor = conn.cursor()
#        cursor.execute("drop table data if exists")
        cursor.execute("create table if not exists data (val)") # sqlite gives us rowid for free.
        self.cache_conn = conn
        self.cache_cursor = cursor
        return
    
    def append_cache(self, values):
        self.cache_cursor.executemany("insert into data values (?)", values)
        return
    
    def close_cache(self):
        self.cache_conn.commit()
        self.cache_conn.close()
        self.cache_conn = None
        self.cache_cursor = None
        return

    def generate(self, rowsPerDayMap):
        for i in range(0, NUM_DAYS):
            numValues = rowsPerDayMap.get(i)
            valsSoFar = 0

            self.open_sqlite_cache(i)
                
            while valsSoFar < numValues:
                valuesThisLoop = min(10_000, numValues - valsSoFar)
                #print("%s: valuesThisLoop = %s" % (i, valuesThisLoop))
                x = []

                # open sqlite
                if self.dataType == tStrLong or self.dataType == tStrCategory:
                    for j in range(valuesThisLoop):
                        x.append(self._genWord(i))
                    #self.d[i] = x
                else:
                    x = self._dist_gen(valuesThisLoop)
                    
                    if self.hasTransition(i, T_VALUES_SCALE, print_debug=True):
                        x = x * 40
                    # endif
                    
                    if self.hasTransition(i, T_ZEROS_HIGH):
                        if random.random() >= 0.5:
                            x = self.replace_half(x, 0)
                    
                    if self.hasTransition(i, T_NULLS_HIGH):
                        if random.random() >= 0.5:
                            x = self.replace_half(x, "")

                    x = self._convert_type(x)
                    
                    if self.hasTransition(i, T_VALUES_SOME_STRINGS):
                        X = []
                        for xx in x:
                            if random.choice([True, False]):
                                xx = "sometimes-" + str(xx)
                            X.append(xx)
                        #self.d[i] = X 
                        x = X
                    elif self.hasTransition(i, T_VALUES_ALL_STRINGS):
                        x = ["all-" + str(xx) for xx in x]
                    # endif
                # endif
                valsSoFar += len(x)
                assert valuesThisLoop == len(x)

                newX = []
                for xx in x:
                    newX.append((xx,))
                self.append_cache(newX)

            #rowCount = len(self.d[i])
            assert valsSoFar == rowsPerDayMap.get(i), "%s: %s != %s but it should be" % (self.fieldName, valsSoFar, rowsPerDayMap.get(i))
            
            self.close_cache()

        # endfor

        return


def mp_start(col:ColumnSpace, rowsPerDayMap):
    print("generating %d days for column %s..." % (len(rowsPerDayMap), col.fieldName))
    col.generate(rowsPerDayMap)
    return

class ColumnSet:
    def __init__(self, cols, rowsPerDayMap):
        self.cols = cols
        procList = []
        for c in self.cols:
            p = Process(target=mp_start, args=(c, rowsPerDayMap))
            p.start()
            procList.append(p)
            #c.generate(rowsPerDayMap)
        # endfor

        for pp in procList:
            print("joining...")
            pp.join()

        print("done joining.")

        self.grid = {}
        self.alert_guide = {}

        self.cache_handles = {} # 
        self.max_rows = {} # fieldName -> max
        self.max_max_rows = 0
        self.rowsPerDayMap = rowsPerDayMap

    def get_header_line(self, day_i):
        h = []
        for c in self.cols:
            h.append(c.get_field_name(day_i))
        return h

    def write_output_to_json(self, numDays, outdir):
        time_now = time.time()
        for dayIndex in range(numDays):
            self._open_caches(dayIndex)
            fname = "%s/%s.json" % (outdir, dayIndex)
            with open(fname, "w") as fp:
                theRecords = []
                fieldNames = self.get_header_line(dayIndex)
                
                for rowIndex in range(1, self.rowsPerDayMap.get(dayIndex)):
                    row = self.get_data_row(fieldNames, rowIndex, noneValue=None)
                    #print("ROW:", row)
                    #assert len(row) == len(fieldNames)
                    if row == [None]: # Grrr. dayIndex needs to start @ 1 and if it starts at 0 we get nonsense.
                        print("Got a nonsense row...")
                        continue

                    d = {}
                    for i in range(len(fieldNames)):
                        d[fieldNames[i]] = row[i]
                    theRecords.append(d)
                
                js = json.dumps(theRecords)
                fp.write(js)
                fp.close()

                day_time = time_now - ((NUM_DAYS - dayIndex - 1) * 86400)
                os.utime(fname, (day_time, day_time)) # set atime, mtime
            # endwith
            self._close_caches()
        return

    def write_output_to_jsonlines(self, numDays, outdir):
        time_now = time.time()
        for dayIndex in range(numDays):
            self._open_caches(dayIndex)
            fname = "%s/%s.jsonl" % (outdir, dayIndex)
            with open(fname, "w") as fp:
                fieldNames = self.get_header_line(dayIndex)
                
                for rowIndex in range(1, self.rowsPerDayMap.get(dayIndex)):
                    row = self.get_data_row(fieldNames, rowIndex, noneValue=None)
                    #print("ROW:", row)
                    #assert len(row) == len(fieldNames)
                    if row == [None]: # Grrr. dayIndex needs to start @ 1 and if it starts at 0 we get nonsense.
                        print("Got a nonsense row...")
                        continue

                    d = {}
                    for i in range(len(fieldNames)):
                        d[fieldNames[i]] = row[i]
                    
                    js = json.dumps(d)
                    fp.write(js)
                    fp.write("\n")

                fp.close()

                day_time = time_now - ((NUM_DAYS - dayIndex - 1) * 86400)
                os.utime(fname, (day_time, day_time)) # set atime, mtime
            # endwith
            self._close_caches()
        return

    def _open_caches(self, dayIndex):
        for c in self.cols:
            fname = c.get_cache_for(dayIndex)
            if fname is None or not os.path.exists(fname):
                print("no entry for in col = %s, dayIndex = %s; fname = %s" % (c.fieldName, dayIndex, fname))
                os._exit(1)
            else:
                conn = sqlite3.connect(fname)
                self.cache_handles[c.fieldName] = conn

                cursor = conn.cursor()
                r = cursor.execute("SELECT max(rowid) FROM data")
                for row in r:
                    self.max_rows[c.fieldName] = row[0]
                    self.max_max_rows = max(self.max_max_rows, row[0])
                    break
            # endif
        # endfor

        print("caches open... max_rows = %s --> %s" % (self.max_rows, self.max_max_rows))
        return

    def _close_caches(self):
        for _, conn in self.cache_handles.items():
            conn.close()
        # endfor
        self.cache_handles = {}
        self.max_rows = {}
        self.max_max_rows = 0
        return

    def get_data_row(self, fieldNames, rowIndex, noneValue=None):
        row = []
        # gets the row for the given opened dayIndex..
        #print("@@@@ ", fieldNames, rowIndex);
        for f in fieldNames:
            #print("f:", f)
            cacheConn = self.cache_handles.get(f)
            if cacheConn is None:
                #print("cc is none");
                row.append(noneValue)
            else:
                cursor = cacheConn.cursor()
                cachedRow = cursor.execute("select val from data where rowid=?", (rowIndex,))
                if cachedRow is None:
                    print("possible error on row %s" % rowIndex)
                    row.append(noneValue)
                else:
                    #print("cachedRow = ", cachedRow)
                    for r in cachedRow:
                        row.append(str(r[0]))
                        #print("r:", r)
                        break
        #print("-->", row)
        return row

    def write_output_to_csv(self, numDays, outdir):
        time_now = time.time()
        # build our output CSV files...
        # FIXME: do it as multiple sessions if sessions=[min,max] is used.
        for dayIndex in range(numDays):
            self._open_caches(dayIndex)
            fname = "%s/%s.csv" % (outdir, dayIndex)
            with open(fname, "w") as fp:
                fieldNames = self.get_header_line(dayIndex)
                fp.write(",".join(fieldNames))
                fp.write("\n")
                
                for rowIndex in range(self.rowsPerDayMap.get(dayIndex)):
                    row = self.get_data_row(fieldNames, rowIndex, noneValue="")
                    #for j in range(len(row)):
                    #    row[j] = str(row[j])
                    fp.write(",".join(row))
                    fp.write("\n")
                fp.close()

                day_time = time_now - ((NUM_DAYS - dayIndex - 1) * 86400)
                os.utime(fname, (day_time, day_time)) # set atime, mtime
            # endwith
            self._close_caches()
        return

    def gen_qa_alert_guide(self):
        """ This generates a dump of what alerts and when that the QA system should look for
            after loading this data.
        """
        #for c in cols:
            # 


def getExtraCols():
    cols = [ 
            ColumnSpace("e-uniform1",    
                         tInt,   0.00, 0,                D_UNIFORM),         # no alert
             
            ColumnSpace("f-uniform1",    
                         tInt,   0.00, 0,                D_UNIFORM),         # no alert
             
            ColumnSpace("g-uniform1",    
                         tInt,   0.00, 0,                D_UNIFORM),         # no alert
             
             ColumnSpace("e-normal1",     
                         tFloat,
                         0.20,
                         T_SCHEMA_NAME,
                         D_NORMAL),          # we change names
             ColumnSpace("f-normal1",     
                         tFloat,
                         0.40,
                         T_SCHEMA_NAME,
                         D_NORMAL),          # we change names
             ColumnSpace("g-normal1",     
                         tFloat,
                         0.60,
                         T_SCHEMA_NAME,
                         D_NORMAL),          # we change names
             
             ColumnSpace("e-uniform2",    
                         tFloat, 0.30, T_VALUES_SCALE,   D_UNIFORM),         # values change - uniform distribution
             ColumnSpace("f-uniform2",    
                         tFloat, 0.60, T_VALUES_SCALE,   D_UNIFORM),         # values change - uniform distribution
             ColumnSpace("g-uniform2",    
                         tFloat, 0.90, T_VALUES_SCALE,   D_UNIFORM),         # values change - uniform distribution
             
             ColumnSpace("e-normal2",    
                         0.70,
                         tInt,
                         T_VALUES_SCALE | T_VALUES_SOME_STRINGS,              # SOMETIMES we insert strings
                         D_NORMAL),
             
             ColumnSpace("f-normal2",    
                         0.80,
                         tInt,
                         T_VALUES_SCALE | T_VALUES_SOME_STRINGS,              # SOMETIMES we insert strings
                         D_NORMAL),
             
             ColumnSpace("g-normal2",    
                         0.90,
                         tInt,
                         T_VALUES_SCALE | T_VALUES_SOME_STRINGS,              # SOMETIMES we insert strings
                         D_NORMAL),
             
             # ALWAYS start inserting strings
             ColumnSpace("e-uniform3",    
                         tFloat,
                         0.90,
                         T_VALUES_ALL_STRINGS,
                         D_UNIFORM),

             # category strings that become long strings.
             ColumnSpace("e-catStr1",
                         tStrCategory,
                         0.5,
                         T_STRING_SMALL_TO_LONG,
                         0),        # could use the prob dist to pick which category str, I suppose...

             # category strings that become long strings.
             ColumnSpace("f-catStr1",
                         tStrCategory,
                         0.6,
                         T_STRING_SMALL_TO_LONG,
                         0),        # could use the prob dist to pick which category str, I suppose...

             # category strings that become long strings.
             ColumnSpace("g-catStr1",
                         tStrCategory,
                         0.7,
                         T_STRING_SMALL_TO_LONG,
                         0),        # could use the prob dist to pick which category str, I suppose...

             # unchanging category string.
             ColumnSpace("e-catStrSteady2",
                         tStrCategory,
                         0,
                         0,
                         0),

             # unchanging category string.
             ColumnSpace("f-catStrSteady2",
                         tStrCategory,
                         0,
                         0,
                         0),

             # unchanging category string.
             ColumnSpace("g-catStrSteady2",
                         tStrCategory,
                         0,
                         0,
                         0),

             # long string becomes a category
             ColumnSpace("e-descStr1",
                         tStrLong,
                         0.5,
                         T_STRING_LONG_TO_SMALL,
                         0),

             # long string becomes a category
             ColumnSpace("f-descStr1",
                         tStrLong,
                         0.7,
                         T_STRING_LONG_TO_SMALL,
                         0),

             # long string becomes a category
             ColumnSpace("g-descStr1",
                         tStrLong,
                         0.9,
                         T_STRING_LONG_TO_SMALL,
                         0)

    ]

    return cols

def getBaseCols():
    cols = [ ColumnSpace("id",
                         tInt, 0.00, 0,                D_INCREMENT),       # 
             
             ColumnSpace("uniform1",    
                         tInt,   0.00, 0,                D_UNIFORM),         # no alert
             
             ColumnSpace("zeroes1",    
                         tInt,   0.60, T_ZEROS_HIGH,     D_UNIFORM),         # zeroes go high
             
             ColumnSpace("nulls1",    
                         tInt,   0.40, T_NULLS_HIGH,     D_UNIFORM),         # zeroes go high
             
             ColumnSpace("normal1",     
                         tFloat,
                         0.20,
                         T_SCHEMA_NAME,
                         D_NORMAL),          # we change names
             
             ColumnSpace("uniform2",    
                         tFloat, 0.30, T_VALUES_SCALE,   D_UNIFORM),         # values change - uniform distribution
             
             ColumnSpace("normal2",    
                         tInt,
                         0.70,
                         T_VALUES_SCALE | T_VALUES_SOME_STRINGS,              # SOMETIMES we insert strings
                         D_NORMAL),
             
             ColumnSpace("normal3",    
                         tInt,
                         0.70,
                         T_VALUES_SCALE,              # SOMETIMES we insert strings
                         D_NORMAL),
             
             # ALWAYS start inserting strings
             ColumnSpace("uniform3",    
                         tFloat,
                         0.90,
                         T_VALUES_ALL_STRINGS,
                         D_UNIFORM),

             # category strings that become long strings.
             ColumnSpace("catStr1",
                         tStrCategory,
                         0.5,
                         T_STRING_SMALL_TO_LONG,
                         0),        # could use the prob dist to pick which category str, I suppose...

             # unchanging category string.
             ColumnSpace("catStrSteady2",
                         tStrCategory,
                         0,
                         0,
                         0),

             # long string becomes a category
             ColumnSpace("descStr1",
                         tStrLong,
                         0.5,
                         T_STRING_LONG_TO_SMALL,
                         0)
          

            # Other tests needed:
                # we should alert on...
                #
                # string changes:
                #     [X] category becomes descriptive
                #     [X] complex becomes simple, etc.
                #         -- will the entropy distance metric work here?
                #
                #     [X]  number becomes a string in later runs...
                #     [X]  number SOMETIMES becomes a string in later runs
                # increase in NULLs
                # increase in 0s
                # 
                # a day in the middle without data
                # day with duplicate data of the day before
                # day with mostly duplicate data of day before (> 50% columns)
                # a corrupted CSV (too many columns)
                # an empty 0-byte CSV
                # JSON payload and parsing/expansion

           ];

    return cols


def gen_number_rows_per_day(numDays):
    d = {}
    for i in range(numDays):
        numVals = int(APPROX_ROWS_PER_DAY + (APPROX_ROWS_PER_DAY * 0.2 * random.random()))
        d[i] = numVals
    
    # pick a random day and screw it up.
    for i in range(numDays):
        if random.random() > 0.8:
            d[i] = int(d[i] / 10)

    return d

def gen_data(outdir=None, numberRows=None, use_json=None, use_jsonl=None, num_days=None):
    global APPROX_ROWS_PER_DAY
    if outdir is None:
        outdir = "data"
    if numberRows is None:
        APPROX_ROWS_PER_DAY = 5000
    else:
        APPROX_ROWS_PER_DAY = int(numberRows)
    if use_json is None:
        use_json = False
    if use_jsonl is None:
        use_jsonl = False

    if num_days is None:
        num_days = 10
    
    global NUM_DAYS
    NUM_DAYS = int(num_days)

    if not os.path.exists(outdir):
        os.mkdir(outdir)
        if not os.path.exists(outdir):
            print("unable to create %s dir; bailing" % outdir)
            sys.exit(2)

    
    print("generating %s rows x %s days" % (APPROX_ROWS_PER_DAY, NUM_DAYS))

    cols = getBaseCols()
    #extraCols = getExtraCols()
    #cols.extend(extraCols)

    rowsPerDayMap = gen_number_rows_per_day(NUM_DAYS)
    cs = ColumnSet(cols, rowsPerDayMap)

    if use_json:
        cs.write_output_to_json(NUM_DAYS, outdir)
    elif use_jsonl:
        cs.write_output_to_jsonlines(NUM_DAYS, outdir)
    else:
        cs.write_output_to_csv(NUM_DAYS, outdir)
    
    print("--done: ~%s x %s x %s --> %s--" % (len(cols), APPROX_ROWS_PER_DAY, NUM_DAYS, len(cols) * sum(rowsPerDayMap.values())))


def rm_cache():
    print("remove cache files...")
    flist = os.listdir(".")
    for f in flist:
        if f.endswith(".cache"):
            print("removing %s" % f)
            os.unlink(f)
    print("--done--")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--out",  help="Out directory")
    ap.add_argument("-r", "--rows", help="Approx number of rows per day")
    ap.add_argument("--days",       help="Number of days of data")
    ap.add_argument("--json",       help="Output JSON format",          action='store_true')
    ap.add_argument("--jsonl",      help="Output JSON-lines format",    action='store_true')
    ap.add_argument("--rm-cache",   help="Remove cache files",          action='store_true')

    #ap.add_argument("-d", "--days", help="Number of days to go back")
    #ap.add_argument("-b", "--base", help="Base file definition to use")

    args = ap.parse_args()
    
    if args.rm_cache:
        rm_cache()
        return

    if args.json and args.jsonl:
        print("You cannot use --json and --jsonl together; pick one.")
        os._exit(2)

    gen_data(args.out, args.rows, args.json, args.jsonl, args.days)

    return

if __name__ == "__main__":
    main()

