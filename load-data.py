#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# load-data.py
# Demo Pipelines
#
# Copyright (c) 2020-2021 Data Culpa, Inc. All rights reserved.
#
# Proprietary and Confidential.  Unauthorized use, copying or dissemination
# of these materials is strictly prohibited.
#

from dataculpa import DataCulpaValidator, DataCulpaConnectionError

import pandas as pd

import argparse
import csv
import os
import random
import string

import sys
import holidays
import time
import traceback
from datetime import datetime, timedelta, timezone

import dotenv

gDC_HOST = None
gDC_PORT = None
gDC_PROTOCOL = None

API_KEY = os.environ.get("DC_DOCKER_DEFAULT_USER")
API_SECRET = os.environ.get("DC_DOCKER_DEFAULT_SECRET")
assert API_KEY is not None
assert API_SECRET is not None

def load_env():
    DC_HOST = os.environ.get("DC_HOST")
    DC_PORT = os.environ.get("DC_PORT")
    DC_PROTOCOL = os.environ.get("DC_PROTOCOL")

    if DC_HOST is None:
        sys.stderr.write("Can't find a DC_HOST setting...\n")
        os._exit(3)

    if DC_PORT is None:
        sys.stderr.write("NO DC_PORT set; using 7778...\n")
    
    if DC_PROTOCOL is None:
        sys.stderr.write("No DC_PROTOCOL set; assuming HTTPS\n");

    global gDC_HOST
    global gDC_PORT
    global gDC_PROTOCOL

    gDC_HOST = DC_HOST
    gDC_PORT = DC_PORT
    gDC_PROTOCOL = DC_PROTOCOL
    return

def nextWatchpointVersion(wName):
    try:
        dc = DataCulpaValidator(wName,
                                dc_host=gDC_HOST,
                                dc_port=gDC_PORT,
                                protocol=gDC_PROTOCOL,
                                api_access_id=API_KEY,
                                api_secret=API_SECRET)
        jr = dc.getWatchpointVariations(wName)
        if jr == []:
            return str(1)

        maxVers = 0
        for entry in jr:
            v = entry.get('version')
            try:
                v = int(v)
            except:
                v = -100 - len(jr)
            maxVers = max(maxVers, v)
        maxVers += 1

        return str(maxVers)
    except DataCulpaConnectionError:
        sys.stderr.write("Couldn't connect to Validator at %s:%s [protocol: %s]\n" % (gDC_HOST, gDC_PORT, gDC_PROTOCOL))
        os._exit(2)


WATCHPOINT_NAME = "timeshift-3"
WATCHPOINT_VERS = None


def GetPerfData(dc:DataCulpaValidator):
    jr = {}
    try:
        url = dc._get_base_url("debug/perf")
        r = dc.GET(url)
        jr = dc._parseJson(url, r.content)
    except:
        traceback.print_exc()
        print("got an error.")
    return jr

gBeforePerf = None

def toNameDict(d):
    r = {}
    for entry in d:
        r[entry.get('name')] = entry
    return r

def convertField(f, old_v, new_v):
    if f in [ "name", "num_restarts" ]:
        return None

    dv_str = "?"

    if f in ["mem_rss", "mem_vms", "max_rss", "io_wb", "io_rb"]:
        dv = new_v - old_v
        old_v  = "%.1f MB" % (old_v / 1024 / 1024)
        new_v  = "%.1f MB" % (new_v / 1024 / 1024)
        dv_str = "%.1f MB" % (dv / 1024 / 1024)

    if f in [ "time_now", "cpu_sys", "cpu_user" ]:
        dv_str = str(int(new_v - old_v)) + " seconds"
        old_v = " "
        new_v = " "

    if f in [ "pid" ]:
        if old_v != new_v:
            dv_str = "restarted"
        else:
            dv_str = "(no change)"

    s = "%16s %10s %10s %10s" % (f, old_v, new_v, dv_str)


    return s

def comparePerf(b, a):
    db = toNameDict(b)
    da = toNameDict(a)

    ks = list(db.keys())
    for k in ks:
        bb = db.get(k)
        aa = da.get(k)
        print(k)
        print("-" * (len(k)))
        fs = list(bb.keys())
        for f in fs:
            line = convertField(f, bb.get(f), aa.get(f))
            if line is not None:
                print(line)
    print("==================")
#    print("BEFORE:", b)
#    print("\n")
#    print("AFTER: ", a)
    return

class QueueActivity:
    def __init__(self, dcValidator:DataCulpaValidator):
        self.queue_start = {}
        self.queue_finished = {}
        self.conn = dcValidator

    def add(self, q):
        self.queue_start[q] = time.time()
        return

    def finish(self, q):
        self.queue_finished[q] = time.time()
        return 

    def get_unfinished(self):
        qs = set(self.queue_start.keys())
        qf = set(self.queue_finished.keys())
        d  = qs.difference(qf)
        return list(d)
    
    def get_durations(self):
        dt = {}
        maxTime = 0
        for k, tf in self.queue_finished.items():
            ts = self.queue_start.get(k)
            d = tf - ts
            dt[k] = d
            maxTime = max(maxTime, d)
        return dt, maxTime

    def load_status(self):
        unfinished = self.get_unfinished()
        if len(unfinished) == 0:
            print("all done!")
            return 0

        #print("----")
        numNowFinished = 0
        unfinished = sorted(list(unfinished))
        for queue_id in unfinished:
            jr = self.conn.validation_status(queue_id)
            #print("queue_id = %s --> %s" % (queue_id, jr))
            if jr.get('status', -1) == 100:
                self.finish(queue_id)
                numNowFinished += 1
        # endfor

        if numNowFinished == len(unfinished):
            return 0
        return len(unfinished) - numNowFinished

def push_data(fp, seconds_ago, jsonXForm, qa:QueueActivity):

    dc = DataCulpaValidator(WATCHPOINT_NAME,
                            watchpoint_version=WATCHPOINT_VERS,
                            protocol=gDC_PROTOCOL, 
                            dc_host=gDC_HOST, 
                            dc_port=gDC_PORT,
                            timeshift=seconds_ago,
                            api_access_id=API_KEY,
                            api_secret=API_SECRET,
                            queue_window=1000)

    #dc.watchpoint_log_message("hello");

    if qa is None:
        qa = QueueActivity(dc)

    global gBeforePerf
    if gBeforePerf is None:
        gBeforePerf = GetPerfData(dc)
        #print(gBeforePerf)

    if not jsonXForm:
        dc.load_csv_file(fp)
        (queue_id, result) = dc.queue_commit()
        qa.add(queue_id)
    else:    
        # OK, do the json.
        with open(fp, "r") as fh:
            cr = csv.DictReader(fh)
            for row in cr:
                #            j.append(row)
                dc.queue_record(row)
            (queue_id, result) = dc.queue_commit()
            qa.add(queue_id)
    # endif

    return qa

def load_data(dir_path, jsonXForm, delayAfterFirstTwo=0):
    if not os.path.exists(dir_path):
        print("missing data dir %s; bailing" % dir_path)
        sys.exit(2)

    print("Pushing data to %s %s..." % (WATCHPOINT_NAME, WATCHPOINT_VERS))

    time_now = time.time()
    mod_times = {}
    files = os.listdir(dir_path)
    for f in files:
        fpath = "%s/%s" % (dir_path, f)
        mtime = os.path.getmtime(fpath)
        #print("%10s %10.0f" % (f, time_now - mtime))
        mod_times[fpath] = time_now - mtime
    
    qa = None
    # Sort by the values of mod_times, reversing so we get the
    # oldest first.
    i = 0
    for fp in sorted(mod_times, key=mod_times.get, reverse=True):
        print(fp, mod_times[fp])
        qa = push_data(fp, mod_times[fp], jsonXForm, qa)
        assert qa is not None
        i += 1
        if i > 1:
            if delayAfterFirstTwo > 0:
                print("Sleeping for %s..." % delayAfterFirstTwo)
                try:
                    time.sleep(delayAfterFirstTwo)
                except KeyboardInterrupt:
                    os._exit(2)

    if i == 0:
        print("No files in the directory %s" % dir_path)
        os._exit(2)
        
    # watch for completion
    while True:
        numberRemaining = qa.load_status()
        print(numberRemaining)
        if numberRemaining == 0:
            break
            
        if numberRemaining < 5:
            time.sleep(numberRemaining)
        else:
            time.sleep(5)

    perfAfter = GetPerfData(qa.conn)
    comparePerf(gBeforePerf, perfAfter)

    return


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-j", "--json", help="Transform the CSV files to test the JSON code path.", action='store_true')
    ap.add_argument("-d", "--data", help="Path to data directory")
    ap.add_argument("-w", "--wait", help="Wait seconds between pushes after the first two")
    args = ap.parse_args()
    
    if args.data is None:
        args.data = "data"
    if not os.path.exists(args.data):
        sys.stderr.write("No existing path %s... run gen-data.py first?\n")
        sys.exit(2)
        return

    load_env()

    global WATCHPOINT_VERS
    WATCHPOINT_VERS = nextWatchpointVersion(WATCHPOINT_NAME)

    w = args.wait
    if w is None:
        w = 0
    else:
        w = int(w)

    ts = time.time()
    load_data(args.data, args.json, w)
    te = time.time()
    dt = te - ts

    print("------")
    print("total time = %.2f seconds" % dt)


    return

if __name__ == "__main__":
    main()

