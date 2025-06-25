#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# GenWords.py
# Demo Pipelines
#
# Copyright (c) 2020-2021 Data Culpa, Inc. All rights reserved.
#
# Proprietary and Confidential.  Unauthorized use, copying or dissemination
# of these materials is strictly prohibited.
#


import csv
import os
import sys
import random
import time


class AmericanEnglishDict:
    def __init__(self):
        # Use local dictionary file in the repository
        script_dir = os.path.dirname(os.path.abspath(__file__))
        FILE = os.path.join(script_dir, "american-english")
        self.fh = open(FILE, "r")
        self.fh_len = os.stat(FILE).st_size

    def _get_random_word(self):
        # seek to a random location and then walk forwards to a \n and then read the next line.
        pos = random.random() * self.fh_len
        self.fh.seek(pos, 0)
        i = 0
        while True:
            # we can trip into Unicode garbage...maybe Swift strings
            # weren't so misguided afterall... Hahaha, just kidding.
            try:
                c = self.fh.read(1)
            except UnicodeDecodeError:
                return "UnicodeIsFun" # let's see how this goes :-)

            if c == None:
                # We hit EOF.
                return None # ask the caller to try again...need a goto.
            if c == '\n':
                break
            # else eat the byte.
            i += 1
            if (i > 1000):
                print("giving up, no idea what is going on here")
                os._exit(2)
        # endwhile

        s = self.fh.readline()
        return s.strip()

    def random_word(self):
        c = 0
        while True:
            s = self._get_random_word()
            if s is None:
                # try again
                pass
            if c > 10:
                print("crazy; giving up")
                os._exit(1)
            c += 1
            return s
        return None


class ValueGenerator:
    def __init__(self):
        self.english = AmericanEnglishDict()

        #self.next_id = 0
        #self.cat_ints = [ 999999 ]
        #for i in range(0, 20):
        #    self.cat_ints.append(i)
        #self.cat_ints[3] = None
        
        self.cat_string = []
        for i in range(0, 10):
            #theList = []
            w = self._random_word()
            if w is None:
                #print("got a None! sheesh")
                w = "je suis"
            #theList.append(w)
            self.cat_string.append(w)
        # endfor

    def _random_word(self):
        return self.english.random_word()

#    def rand_bool(self):
#        return 1 if random.random() >= 0.5 else 0
    
#    def rand_cat_int(self):
#        return random.choice(self.cat_ints)

#    def rand_float(self, _max):
#        return random.random() * _max
    
    def rand_cat_str(self):
        return random.choice(self.cat_string)

    def rand_words(self, _n):
        theList = []
        for i in range(0, _n):
            w = self._random_word()
            if w is None:
                w = "NULL"
            theList.append(w)
        return " ".join(theList)

