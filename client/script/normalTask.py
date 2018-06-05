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
from scipy.stats import itemfreq
import datetime;
import os
import fileinput

#files should be in this format:
#key1,val1,val2,val3....valn
#key2,val2,val2,val3....valn
#keyn....
#this python script computer results like this:
#key1, avg_val1,low_ci,high_ci,avg_val2,low_ci,high_ci....
#key2....
#keyn...

def ci(D):
    s = np.array(D);
    n, min_max, mean, var, skew, kurt = st.describe(s)
    std=math.sqrt(var)

    #note these are sample standard deviations
    #and sample variance values
    #to get population values s.std() and s.var() will work


    #The location (loc) keyword specifies the mean.
    #The scale (scale) keyword specifies the standard deviation.

    # We will assume a normal distribution
    R = st.norm.interval(0.05,loc=mean,scale=std)
    return R;

def avg_ci(l):
    return [np.average(l), ci(l)];


if __name__ == "__main__":
    usage = "python parseTasks.py file...\n file in format index1,val1,val2.."
    if (len(sys.argv) < 2):
        print usage
        exit();
    kv = {}
    avg_kv = {}
    for l in fileinput.input():
        if (l.startswith('#')):
            continue;
        t = l.rstrip('\n').split(':')
        for i in range(0,len(t)):
            t[i] = float(t[i]);
        if (not kv.has_key(t[0])):
            kv[t[0]] = [];
            for i in range(1,len(t)):
                kv[t[0]].append([t[i]]);
        else:
            try:
                for i in range(1, len(t)):
                    kv[t[0]][i-1].append(t[i]);
            except:
                print "add index " + str(i) + " of " + str(t) + "to dict " + str(kv[t[0]])
                exit(0);

    ret = []
    for k in sorted(kv.keys()):
        i = kv[k];
        if (i[5][0] > 0 and i[5][1] > 0):
            #print "%d, %.2f" % (k, i[5][0] / i[5][1]);
            nrt = (int)((i[5][0] / i[5][1]) *  100);
            ret.append(nrt);
#            if (nrt == 807):
#                print k;
#                print i;

    dist = itemfreq(ret);
    for i in dist:
        print "%.2f, %d" % (((float)(i[0]))/100, i[1]);
#         avg_kv[k] = [];
#         out_str = "%d,\t%d" % (k, avg_ci(kv[k][0])[0]);
#         for i in range(1, len(kv[k])):
#             #medium low 25 75 max
#             out_str += ",\t%.2f,\t%.2f,\t%.2f,\t%.2f,\t%.2f" % (np.percentile(kv[k][i],50),np.percentile(kv[k][i],0),np.percentile(kv[k][i],10),np.percentile(kv[k][i],90),np.percentile(kv[k][i],100));
# #            out_str += ",\t%.2f,\t%.2f,\t%.2f" % (avg_ci(kv[k][i])[0],avg_ci(kv[k][i])[1][0],avg_ci(kv[k][i])[1][1])
# #            if (i == 6 and avg_ci(kv[k][i])[1][1] - avg_ci(kv[k][i])[1][0] > 10):
# #                sys.stderr.write("ci > 10 for key %d line %d \n" % (k,i));
#         print out_str;
