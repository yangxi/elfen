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

#    self.current.write(struct.pack('fffIB', timestamp, latencyMS, queueTimeMS, totalHitCount, len(taskString)))

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

def norfreq_to_timefreq(nfreq):
    bin_array = nfreq[0];
    bin_time = np.array(nfreq[0]) * np.array(nfreq[1]);
    total_time = np.sum(bin_time);
    bin_time_freq = bin_time / total_time
    accu_time = 0;
    accu_array = []
    revers_accu_array = []

    for i in range(0, len(bin_array)):
        revers_accu_array.append(1 - accu_time);
        accu_time += bin_time_freq[i];
        accu_array.append(accu_time);

    return [bin_array, bin_time_freq, accu_array, revers_accu_array]




def parse_header_line(l):
    typeToDesc={"long":"Q", "int":"I", "byte":"b", "float":"f", "unsigned char":"B"};
    typeToSize={"long":8, "int":4, "byte":1, "float":4, "unsigned char":1};
    d = {}
    t = "="
    s = 0;
    a = l.strip('#').rstrip('\n').split(',')
    for i in range(0, len(a)):
        kv = a[i].split('->')
        d[kv[0]]=i+1;
        t += typeToDesc[kv[1]];
        s += typeToSize[kv[1]];
    print "this file has keys " + str(d.keys()) + " line size " + str(s) + " parse with " + t;
    return (d, t, s);

def update_latency_stat(stat,latency):
    for k in latency:
        stat[k].append(latency[k]);

def latency(vals):
    average_latency = np.average(vals);
    sorted_vals = sorted(enumerate(vals),key=lambda i:i[1])
#    sorted_vals = sorted(vals);
    mean_index = int(len(sorted_vals)*0.5)
    per_95_index = int(len(sorted_vals)*0.95)
    per_99_index = int(len(sorted_vals)*0.99)
    return {"avg":average_latency, "50":sorted_vals[mean_index][1], "95":sorted_vals[per_95_index][1], "99":sorted_vals[per_99_index][1], "perc_index":[sorted_vals[mean_index][0],sorted_vals[per_95_index][0],sorted_vals[per_99_index][0]]};

def parse_log(fname):
#{col_num:"str"}
    col_key={}
    key_col={}
#[in column view]
    cols=[];
    diff_cols=[]
#[in raw view]
    raws=[];
    iters_index = [];

    f = open(fname,'r');
    hl = f.readline();
    hl = "#taskid:hits:receiveStamp:processStamp:finishStamp:retiredIns:retiredCycles:clienttime:serverQtime:serverPtime:serverLatency\n"
    a = hl.strip('#').rstrip('\n').split(':')
    for i in range(0, len(a)):
        col_key[i] = a[i];
        key_col[a[i]] = i;
    for i in col_key.keys():
        cols.append([]);
    while True:
        r = f.readline().rstrip('\n').split(':');
        if (r[0] == ''):
            print "finished passing the log\n"
            break;
        for i in range(0, len(r)):
            r[i] = int(r[i]);
        r[-1] = r[-1] & 0xffffffff
        r[-2] = r[-2] & 0xffffffff
        raws.append(r);
        for j in range(0, len(r)):
            cols[j].append(r[j]);

    nr_iters = len(raws) / 1141;
    for i in range(0, nr_iters):
        iters_index.append(i * 1141);

    cols[key_col["serverLatency"]] = (np.array(cols[key_col["finishStamp"]]) - np.array(cols[key_col["receiveStamp"]]))/(1000 * 1000)
    cols[key_col["serverPtime"]] = (np.array(cols[key_col["finishStamp"]]) - np.array(cols[key_col["processStamp"]]))/(1000 * 1000)
    cols[key_col["serverQtime"]] = (np.array(cols[key_col["processStamp"]]) - np.array(cols[key_col["receiveStamp"]]))/(1000 * 1000)

    f = open('./rtime.csv', 'w')
    f.write("#no id clientlatency serverlatency serverPtime serverQtime\n");
    for i in range(0, len(cols[key_col["taskid"]])):
        f.write("%d,%d,%d,%d,%d,%d\n" % (i+1, cols[key_col["taskid"]][i], cols[key_col["clienttime"]][i], cols[key_col["serverLatency"]][i], cols[key_col["serverPtime"]][i], cols[key_col["serverQtime"]][i]));
    f.close();


    #marsk retiredCycles and retiredIns
    # cycle_index = key_col["retiredCycles"];
    # cycle_col = cols[cycle_index]
    # cols[cycle_index] = np.array(cycle_col) & (0xffffffff);

    # ins_index = key_col["retiredIns"];
    # ins_col = cols[ins_index]
    # cols[ins_index] = np.array(ins_col) & (0xffffffff);

    for i in range(0, len(cols)):
        c = cols[i];
        diff_c = np.array(c[1:]) - np.array(c[0:-1]);
        for i in range(0, len(diff_c)):
            if (diff_c[i] < 0):
                diff_c[i] = 0xffffffff + diff_c[i];
        diff_cols.append(diff_c);
#    print "%ld - %ld = %ld" % (cols[3][1],cols[3][0], diff_cols[3][0]);
    return {"col_key":col_key,"key_col":key_col,"cols":cols, "diff_cols":diff_cols,"raws":raws, "iters":iters_index};




#input:a list, out put:[bin,percent,accupercent]
def norfreq(a):
    fa = stat.itemfreq(a)
    t = len(a)
    sfa = np.hsplit(fa,2)
    sfa[1] = sfa[1].astype(float)/t;

    x=[];y=[];z=[]
    tz = 0;
    for i in range(0, len(sfa[0])):
        k = sfa[0][i][0]
        v = sfa[1][i][0]
        x.append(k)
        y.append(v)
        tz += v
        z.append(tz);
    return [x,y,z]



#{key->count}
def parse_lucene_iter(parsed_log):
    #process time distribution, latency distribution, queue time distribution
    print parsed_log["key_col"];
    cols = parsed_log["cols"]
    raws = parsed_log["raws"]
    diff_cols = parsed_log["diff_cols"]
    receive_stamp_index = parsed_log["key_col"]["receiveStamp"]
    process_stamp_index = parsed_log["key_col"]["processStamp"];
    finish_stamp_index = parsed_log["key_col"]["finishStamp"];
    cycle_index = parsed_log["key_col"]["retiredCycles"];
    instruction_index = parsed_log["key_col"]["retiredIns"];

    ptimeNS = np.array(cols[finish_stamp_index]) - np.array(cols[process_stamp_index]);
    ptimeMS = ptimeNS/(1000*1000);
    ptime_hist = norfreq(ptimeMS);
    ptime_time_hist = norfreq_to_timefreq(ptime_hist);
    ptime_perc = latency(ptimeNS);

    ltimeNS = np.array(cols[finish_stamp_index]) - np.array(cols[receive_stamp_index]);
    ltimeMS = ltimeNS/(1000*1000);
    ltime_hist = norfreq(ltimeMS);
    ltime_perc = latency(ltimeNS);



    idletimeNS = diff_cols[process_stamp_index] * 2 - diff_cols[cycle_index];
#    print diff_cols[process_stamp_index][0:10]
#    print diff_cols[cycle_index][0:10]
#    print idletimeNS[0:10]
#    for i in range(0,len(idletimeNS)):

        # if (idletimeNS[i] < 0):
        #     print "[%d %d] %d %s %s %s\n" % (diff_cols[process_stamp_index][i], diff_cols[cycle_index][i],i, str(raws[i]), str(raws[i+1]), str(np.array(raws[i+1])-np.array(raws[i])));
        # if (idletimeNS[i] > 1000000):
        #     l = "+%d--" % i;
        #     for j in range(0,10):
        #         l += ",[%d,%d,%d] " % (idletimeNS[i+j], ptimeMS[i+j+1], ltimeMS[i+j+1])
        #     print l
        # if (idletimeNS[i] < 5000):
        #     print "-%d---%s,%s,%s\n" % (i, idletimeNS[i],idletimeNS[i+1],idletimeNS[i+2]);
    idletimeUS = idletimeNS/(1000)
#    print idletimeUS[0:10]
    idletime_hist = norfreq(idletimeUS);
    idletime_perc = latency(ltimeNS);

#    ipkc = np.array(diff_cols[instruction_index]) * 1000 / np.array(diff_cols[cycle_index]);
#    for i in range(0, len(ipkc)):
#        print "%d : %d, %d" % (i, ipkc[i], ptimeMS[i])
    ipkc = (np.array(cols[cycle_index]) * 1000)/np.array(cols[instruction_index]);
    ipkc_hist = norfreq(ipkc);
    ipkc_perc = latency(ipkc);

    #idle report




#    print ltime_perc;
#    for i in range(0, len(ltimeNS)):
#        if (ltimeNS[i] >= ltime_perc["99"]):
#            print "id:%d -> ptime %d, ltime %d diff %s" % (raws[i][0],ptimeMS[i], ltimeMS[i], np.array(raws[i]) - np.array(raws[i-1]));


    return {"ptime_time_hist":ptime_time_hist, "ptime_hist":ptime_hist,"ptime_perc":ptime_perc,"ltime_hist":ltime_hist,"ltime_perc":ltime_perc,"idletime_hist":idletime_hist,"idletime_perc":idletime_perc, "ipkc_hist":ipkc_hist,"ipkc_perc":ipkc_perc};




def parse_lucene_log(fname, expected_qps, expected_iter):

#let's calculate process time distribution
    parsed_log = parse_log(fname);
    nr_iters = len(parsed_log["iters"]);
    nr_tasks = len(parsed_log["raws"]);
    rc_stamp_index = parsed_log["key_col"]["receiveStamp"];
    finish_stamp_index = parsed_log["key_col"]["finishStamp"];
    process_stamp_index = parsed_log["key_col"]["processStamp"];
    cycles_index = parsed_log["key_col"]["retiredCycles"];
    client_time = parsed_log["key_col"]["clienttime"];
    raws = parsed_log["raws"]
    cols = parsed_log["cols"]
    wall_total_cycle = cols[finish_stamp_index][-1] - cols[rc_stamp_index][0];
    wall_total_sec = wall_total_cycle/(1000000000);
    avg_qps = nr_tasks / wall_total_sec;
# observed QPS
    print "iters:%d (%d), tasks:%d, cycles:%d, qps:%f (%d)\n" % (nr_iters, expected_iter, nr_tasks, wall_total_cycle, avg_qps, expected_qps);
# CPU utilization
    diff_cycle_col = parsed_log["diff_cols"][cycles_index]
    nr_cycles = np.sum(diff_cycle_col);
    nr_wall_cycles = cols[process_stamp_index][-1] - cols[process_stamp_index][0]
    utilization = nr_cycles/(nr_wall_cycles * 2.0);
    print "total cycles:%d, total wall cycles:%d, utilization:%f\n" % (nr_cycles, nr_wall_cycles * 2, utilization);

    stat =  parse_lucene_iter(parsed_log);
    stat["measured_qps"] = avg_qps;
    stat["utilization"] = utilization;
    return stat

# process t


    # #stats{qtime:{"avg":[iter0,....,iterN],"mean":[],"}}
    # qtime_stat = {"avg":[],"mean":[],"95":[],"99":[]}
    # ptime_stat = {"avg":[],"mean":[],"95":[],"99":[]}
    # ltime_stat = {"avg":[],"mean":[],"95":[],"99":[]}

    # stats = {"qtime":qtime_stat, "ptime":ptime_stat, "ltime":ltime_stat}
    # avgs = {"qtime":{}, "ptime":{}, "ltime":{}}
    # for i in range(0, len(iters_index)):
    #     start_index = iters_index[i];
    #     end_index = start_index + nr_tasks;
    #     qtime_latency = latency(cols[3][start_index:end_index]);
    #     update_latency_stat(qtime_stat, qtime_latency);
    #     ptime_latency = latency(cols[4][start_index:end_index]);
    #     update_latency_stat(ptime_stat, ptime_latency);
    #     ltime_latency = latency(np.array(cols[3][start_index:end_index]) + np.array(cols[4][start_index:end_index]))
    #     update_latency_stat(ltime_stat, ltime_latency);
    # for t in stats.keys():
    #     for l in stats[t].keys():
    #         avgs[t][l] = [np.average(stats[t][l]),ci(stats[t][l])]
    # return (col_key, raws, cols, stats, avgs)

#each file name in format as
#id_qps_iteration
def parse_logs(names):
    logs = {}
    for f in names:
        sf = f.split('_');
        iters = int(sf[-1])
        qps = int(sf[-2])
        print "parse log file %s qps %d invoks %d\n" %(f, qps, iters)
        logs[qps] = parse_lucene_log(f, qps, iters)
    return logs




if __name__ == "__main__":
    usage = "python logfile qps-latency.csv"
    if (len(sys.argv) < 2):
        print usage
        exit()
    fnames=[]
    logs = parse_logs(sys.argv[1:])
    #output qps-latency.csv

    f = open('./ptime-time-dist.csv', 'w')
    qps = sorted(logs.keys())[0];
    f.write("#timestamp:%s bin qps %d, percentage_time, accumulated_time\n" % (datetime.datetime.now(),qps));
    ptime_dist = logs[qps]["ptime_time_hist"];
    for i in range(0, len(ptime_dist[0])):
        f.write("%d, %.3f, %.3f\n" % (ptime_dist[0][i],ptime_dist[1][i],ptime_dist[2][i]));
    f.close()

    # f = open('./rtime.csv', 'w')
    # q = logs.keys()[0];
    # clienttime_index = logs[q]["log"]["key_col"]["clienttime"]
    # clienttime_data = logs[q]["log"]["cols"][clienttime_idnex];
    # serverlatency_index = logs[q]["log"]["key_col"]["server"]

#how many requests we have
    if (True):
        f = open('./qps-latency.csv', 'w');
        f.write("#timestamp:%s qps,realqps,ptime_50latency,ptime_95latency,ltime_50,ltime_95,CPU utiliztion,IPC\n" % datetime.datetime.now());
        bl = ""
        for qps in sorted(logs.keys()):
            realqps = logs[qps]["measured_qps"];
            cpu_util = logs[qps]["utilization"];
            ipc = logs[qps]["ipkc_perc"]["50"];
            ptime_50 = logs[qps]["ptime_perc"]["50"]/(1000.0*1000);
            ptime_95 = logs[qps]["ptime_perc"]["95"]/(1000.0*1000);
            ptime_99 = logs[qps]["ptime_perc"]["99"]/(1000.0*1000);
            ltime_50 = logs[qps]["ltime_perc"]["50"]/(1000.0*1000);
            ltime_95 = logs[qps]["ltime_perc"]["95"]/(1000.0*1000);
            ltime_99 = logs[qps]["ltime_perc"]["99"]/(1000.0*1000);
            bl += " [%d]=%d " %(qps,100-ltime_99);
            f.write("%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d\n" % (qps,realqps,ptime_50,ptime_95,ptime_99,ltime_50,ltime_95,ltime_99,cpu_util,ipc));
        f.write("#budgets=( %s )\n" %bl);
        for qps in sorted(logs.keys()):
            f.write("#ltime_per_index=" + str(logs[qps]["ltime_perc"]["perc_index"]) + " ptime_index " + str(logs[qps]["ptime_perc"]["perc_index"]) + " \n");
        f.close()

    if (True):
        f = open('./ptime-dist.csv', 'w')
        f.write("#timestamp:%s bin, percentage, accumulated percentage\n" % datetime.datetime.now());
        ptime_dist = logs[qps]["ptime_hist"];
        for i in range(0, len(ptime_dist[0])):
            f.write("%d, %.3f, %.3f\n" % (ptime_dist[0][i],ptime_dist[1][i],ptime_dist[2][i]));
        f.close()


    if (True):
#    if (os.path.isfile('./idletime-dist.csv') == False):

        f = open('./idletime-dist.csv', 'w')
        f.write("#QPS:120 timestamp:%s bin, percentage, accumulated percentage\n" % datetime.datetime.now());
        ptime_dist = logs[qps]["idletime_hist"];
        for i in range(0, len(ptime_dist[0])):
            f.write("%d, %.3f, %.3f\n" % (ptime_dist[0][i],ptime_dist[1][i],ptime_dist[2][i]));
        f.close()
