#!/usr/bin/env python3 
import os
import time
import sys


path = sys.argv[1]
files = os.listdir(path)
mtimes = []
for f in files:
    mtimes.append(os.path.getmtime("%s/%s" % (path, f)))

min_t = max(mtimes)
tn = time.time()
if min_t > tn:
    dt = min_t - tn
else:
    dt = tn - min_t

print(dt, dt / 86400)

#ass = {}
#
#mtimes.sort()
#for mt in mtimes:
#    ass[mt] = mt - dt
#    print(time.ctime(mt), time.ctime(mt - dt))
#os._exit(1)

for f in files:
    fp = "%s/%s" % (path, f)
    mt = os.path.getmtime(fp)
    new_t = mt - dt
    os.utime(fp, (new_t, new_t))

print("done")
