import sys
import json
import numpy as np
import scipy as sp
from scipy import stats as st
import operator
from struct import *
import bisect
import math
import scipy.stats as stat
import datetime;
import os
import fileinput


if __name__ == "__main__":
    usage = "python analyseTop200.py file...\n"
    if (len(sys.argv) < 2):
        print usage
        exit();
    top200stat = {};
    for l in fileinput.input():
        if (l.startswith('#')):
            continue;
        t = l.rstrip('\n').split(',')
        tid = int(t[1]);
        if (top200stat.has_key(tid)):
            top200stat[tid] += 1;
        else:
            top200stat[tid] = 1;
    top200freq = [];
    print "#nr_appear, nr_request, percent"
    for n in range(1, 101):
        nr_app = 0;
        for k in top200stat.keys():
            if (top200stat[k] == n):
#                if (n == 100):
#                    print k
                nr_app += 1;
        top200freq.append(nr_app);
        print "%d, %d, %.4f" % (n, nr_app, nr_app/float(20000))
#    print top200freq;
