#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import cStringIO
import random
import codecs
import socket
import Queue
import sys
import time
import threading
import cPickle
import gc
import struct

# If targetQPS is 'sweep' then we start at this QPS:
SWEEP_START_QPS = 10

# ... and every this many seconds we see if we can increase the target:
SWEEP_CHECK_EVERY_SEC = 60

# More frequent thread switching:
sys.setcheckinterval(10)

# We don't create cyclic garbage, and we want no hiccups:
gc.disable()

MAX_BYTES = 70

# TODO
#   - generalize this to send requests via http too
#   - run hiccup thread here?
#   - sweep to find capacity...
#   - test PK lookup, NRT as well

# python -u perf/sendTasks.py /l/util/wikimedium500.tasks localhost 7777 10 10 10

class RollingStats:

  def __init__(self, count):
    self.buffer = [0] * count
    self.sum = 0
    self.upto = 0

  def add(self, value):
    if value < 0:
      raise RuntimeError('values should be positive')
    idx = self.upto % len(self.buffer)
    self.sum += value - self.buffer[idx]
    self.buffer[idx] = value
    self.upto += 1

  def get(self):
    if self.upto == 0:
      return -1.0
    else:
      if self.upto < len(self.buffer):
        v = self.sum/self.upto
      else:
        v = self.sum/len(self.buffer)
      # Don't let roundoff error manifest as -0.0:
      return max(0.0, v)

class Results:

  def __init__(self, savFile):
    self.buffers = []
    self.current = cStringIO.StringIO()
    self.fOut = open(savFile, 'wb')
    self.current.write("#taskID->int,totalHitCount->int,receiveStamp->long,processStamp->long,finishStamp->long,retiredInstruction->long,unhaltedCycles->long,clientLatency->int\n");
    self.nr_results = 0;
  def add(self, reply):
    self.nr_results += 1;
    self.current.write(reply+"\n")
    if self.current.tell() >= 64*1024:
      self.fOut.write(self.current.getvalue())
      self.fOut.flush()
      self.current = cStringIO.StringIO()
  # def add(self, taskString, totalHitCount, timestamp, latencyMS, queueTimeMS, processTimeMS):
  #   self.nr_results += 1;
  #   self.current.write(struct.pack('ffffIB', timestamp, latencyMS, queueTimeMS, processTimeMS, totalHitCount, len(taskString)))
  #   self.current.write(taskString)
  #   if self.current.tell() >= 64*1024:
  #     self.fOut.write(self.current.getvalue())
  #     self.fOut.flush()
  #     self.current = cStringIO.StringIO()

  def finish(self):
    if (self.current.tell() > 0):
      self.fOut.write(self.current.getvalue());
      self.fOut.flush()
    self.fOut.close()

class SendTasks:

  def __init__(self, serverHost, serverPort, out, runTimeSec, savFile, randombackup):
    self.startTime = time.time()

    self.out = out
    self.runTimeSec = runTimeSec
    self.results = Results(savFile)
    #map {taskID -> (startTime, task)}
    self.sent = {}
    #element: (startTime, taskString)
    self.queue = Queue.Queue()
    self.lock = threading.Lock();
    
    self.randombackup = randombackup;
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.sock.connect((serverHost, serverPort))
    self.backupSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
#    self.backupSock.connect((serverHost, serverPort + 1))
    self.backupSock.connect(("td2", serverPort))

    t = threading.Thread(target=self.gatherResponses,
                         args=())
    t.setDaemon(True)
    t.start()
    t = threading.Thread(target=self.gatherDupResponses,
                         args=())
    t.setDaemon(True)
    t.start()

    t = threading.Thread(target=self.sendRequests,
                         args=())
    t.setDaemon(True)
    t.start()

    self.taskID = 0

  def send(self, startTime, task):
    taskString = task + ";" + str(self.taskID);
    taskString = taskString + ((MAX_BYTES-len(taskString))*' ')
#    print 'taskString is ' + taskString;
    self.sent[self.taskID] = (startTime, taskString);
    self.queue.put((startTime,taskString))
    self.taskID += 1

  def gatherDupResponses(self):
    while True:
      result = ''
      while len(result) < 103:
        result = result + self.backupSock.recv(103 - len(result))
#      print result
      taskID, totalHitCount, receiveStamp, processStamp, finishStamp, retiredIns, unhaltedCycles = result.split(':')
#      l = long(finishStamp) - long(receiveStamp)
#      print 'receive backup request ' + taskID + ":" + str(l) + "[" + str(receiveStamp) + "," + str(finishStamp) + "]";
      taskID = int(taskID)
      self.lock.acquire();
      if (self.sent.has_key(taskID) == False):
        self.lock.release();
        continue;
#      print ('backup returns first' + str(taskID));
#      queueTimeNS = long(processStamp) - long(receiveStamp);
#      processTimeNS = long(finishStamp) - long(processStamp);
#taskID, totalHitCount, queueTimeNS, processTimeNS = result.split(':')
      try:
        taskStartTime, taskString = self.sent[taskID]
      except KeyError:
        print 'WARNING: ignore back bad return taskID=%s' % taskID
        self.lock.release();
        continue
      print '!##';
      del self.sent[taskID];
#      totalHitCount = int(totalHitCount)
#      queueTimeNS = int(queueTimeNS)
#      queueTimeMS = queueTimeNS/1000000.0
#      processTimeNS = int(processTimeNS);
#      processTimeMS = processTimeNS/1000000.0
      endTime = time.time()
#      intSec = int(endTime)
#      if intSec != lastSec:
#        if intSec - self.startTime >= 1:
#          actualQPSStats.add(float(queriesThisSec))
#        queriesThisSec = 1
#        lastSec = intSec
#      else:
#        queriesThisSec += 1


      latencyMS = (endTime-taskStartTime)*1000
#      result.append(str(latencyMS));
#      queueTimeStats.add(queueTimeMS)
#      totalTimeStats.add(latencyMS)
      self.results.add(result+(": %d" % latencyMS));
#      self.results.add(taskString.strip(),
#                       totalHitCount,
#                       taskStartTime-startTime,
#                       latencyMS,
#                       queueTimeMS,
#                       processTimeMS)
      self.lock.release();
      # now = time.time()
      # if now - lastPrint > 2.0:
      #   pctDone = 100.0*(now - startTime) / self.runTimeSec
      #   if pctDone > 100.0:
      #     pctDone = 100.0
      #   self.out.write('%6.1f s: %5.1f%%: %5.1f qps in; %5.1f qps out; %6.1f/%6.1f ms [%d, %d]\n' % \
      #                  (now - startTime, pctDone,
      #                   self.taskID/(now-startTime),
      #                   actualQPSStats.get(),
      #                   totalTimeStats.get(),
      #                   queueTimeStats.get(),
      #                   self.queue.qsize(),
      #                   len(self.sent)))
      #   #self.out.flush()
      #   lastPrint = now


  def gatherResponses(self):

    '''
    Runs as dedicated thread gathering results coming from the server.
    '''

#    startTime = self.startTime
#    lastPrint = self.startTime

#    lastSec = None
#    queriesThisSec = 0

#    queueTimeStats = RollingStats(100)
#    totalTimeStats = RollingStats(100)
#    actualQPSStats = RollingStats(5)

    while True:
      result = ''
      while len(result) < 103:
        result = result + self.sock.recv(103 - len(result))
        #      print result
      taskID, totalHitCount, receiveStamp, processStamp, finishStamp, retiredIns, unhaltedCycles = result.split(':')
#      if (int(taskID) % 100 == 0):
#        l = long(finishStamp) - long(receiveStamp);
#        print 'receive request ' + taskID + ":" + str(l) + "[" + str(receiveStamp) + "," + str(finishStamp) + "]";
      taskID = int(taskID)
      self.lock.acquire();
      if (self.sent.has_key(taskID) == False):
        self.lock.release();
        continue;
        #      queueTimeNS = long(processStamp) - long(receiveStamp);
        #      processTimeNS = long(finishStamp) - long(processStamp);
        #taskID, totalHitCount, queueTimeNS, processTimeNS = result.split(':')
      try:
        taskStartTime, taskString = self.sent[taskID]
      except KeyError:
        print 'WARNING: ignore bad return taskID=%s' % taskID
        self.lock.release();
        continue
      del self.sent[taskID]

#      totalHitCount = int(totalHitCount)
#      queueTimeNS = int(queueTimeNS)
#      queueTimeMS = queueTimeNS/1000000.0
#      processTimeNS = int(processTimeNS);
#      processTimeMS = processTimeNS/1000000.0
      endTime = time.time()
#      intSec = int(endTime)
#      if intSec != lastSec:
#        if intSec - self.startTime >= 1:
#          actualQPSStats.add(float(queriesThisSec))
#        queriesThisSec = 1
#        lastSec = intSec
#      else:
#        queriesThisSec += 1


      latencyMS = (endTime-taskStartTime)*1000
#      result.append(str(latencyMS));
#      queueTimeStats.add(queueTimeMS)
#      totalTimeStats.add(latencyMS)
      self.results.add(result+(": %d" % latencyMS));
      self.lock.release();
#      self.results.add(taskString.strip(),
#                       totalHitCount,
#                       taskStartTime-startTime,
#                       latencyMS,
#                       queueTimeMS,
#                       processTimeMS)

      # now = time.time()
      # if now - lastPrint > 2.0:
      #   pctDone = 100.0*(now - startTime) / self.runTimeSec
      #   if pctDone > 100.0:
      #     pctDone = 100.0
      #   self.out.write('%6.1f s: %5.1f%%: %5.1f qps in; %5.1f qps out; %6.1f/%6.1f ms [%d, %d]\n' % \
      #                  (now - startTime, pctDone,
      #                   self.taskID/(now-startTime),
      #                   actualQPSStats.get(),
      #                   totalTimeStats.get(),
      #                   queueTimeStats.get(),
      #                   self.queue.qsize(),
      #                   len(self.sent)))
      #   #self.out.flush()
      #   lastPrint = now

  def sendRequests(self):

    '''
    Runs as dedicated thread, sending requests from the queue to the
    server.
    '''
    #send "start" message to the server indicating that we are going to start to send requests.
    #self.sock.send("START//\n");

    nr_send = 0;
    while True:
      # wait for 40ms
      try:
        timeTask = self.queue.get(timeout = 0.40)
      except:
#        print "The Queue is Empty.";
#        print len(task.keys());
        continue;
      nr_send += 1;
      sendTime = timeTask[0];
      # the task string 
      task = timeTask[1];
      startTime = time.time()
      while len(task) > 0:
        sent = self.sock.send(task)
        if (self.randombackup != 0):
          if ((nr_send - 1) % self.randombackup == 0):
            print 'Send backup request' + task;
            backupSent = self.backupSock.send(task)

#        print 'sendTime:%.10f, startTime:%.10f, nowTime:%.10f\n' %(sendTime, startTime, time.time());
        if sent <= 0 and backupSend <= 0:
          raise RuntimeError('failed to send task "%s"' % task)
        task = task[sent:]

def pruneTasks(taskStrings, numTasksPerCat):
  byCat = {}
  for s in taskStrings:
    cat = s.split(':', 1)[0]
    if cat not in byCat:
      byCat[cat] = []
    l = byCat[cat]
    if len(l) < numTasksPerCat:
      l.append(s)

  prunedTasks = []
  for cat, l in byCat.items():
    prunedTasks.extend(l)

  return prunedTasks

def run(tasksFile, serverHost, serverPort, meanQPS, numTasksPerCat, runTimeSec, savFile, iteration, out, handleCtrlC, randomshuffle, randombackup):

  recentLatencyMS = 0
  recentQueueTimeMS = 0

  iteration = int(iteration)
  print "run %d iteration" %iteration
  out.write('Mean QPS %s\n' % meanQPS)

  f = open(tasksFile, 'rb')
  # where we push read query strings
  taskStrings = []
  wantline = 1000;
  nrtask = 0;
  while True:
    l = f.readline()
    if l == '':
      break
    orig = l
    idx = l.find('#')
    if idx != -1:
      l = l[:idx]
    l = l.strip()
    if l == '':
      continue
    s = l;
    nrtask += 1;
#    print s
    if len(s) > MAX_BYTES:
      raise RuntimeError('task is > %d bytes: %s' % (MAX_BYTES, l))
#    s = s + ((MAX_BYTES-len(s))*' ')
    taskStrings.append(s)

  r = random.Random(0)
  if (randomshuffle == "shuffle"):
    print("shuffle requests width seed 0\n");
    r.shuffle(taskStrings)
    for s in taskStrings[0:1000]:
      print("%s" %s);
    exit(0);

#  out.write('%d tasks\n' % len(taskStrings))
#  print taskStrings
#  exit(0);

#  taskStrings = pruneTasks(taskStrings, numTasksPerCat)
#  out.write('%d tasks after prune\n' % len(taskStrings))

  # Shuffle again (pruneTasks collates):
#  r.shuffle(taskStrings)

  tasks = SendTasks(serverHost, serverPort, out, runTimeSec, savFile, randombackup)

  targetTime = tasks.startTime

  if meanQPS == 'sweep':
    doSweep = True
    print 'Sweep: start at %s QPS' % SWEEP_START_QPS
    meanQPS = SWEEP_START_QPS
    lastSweepCheck = time.time()
  else:
    doSweep = False

  try:

    warned = False
    iters = 0

    while True:
# we can do something here before this new iteration
      iters += 1

      for task in taskStrings:
# exp distribution
        targetTime += r.expovariate(meanQPS)

        pause = targetTime - time.time()

        if pause > 0:
#          print 'sent %s; sleep %.3f sec' % (task, pause)
          time.sleep(pause)
          warned = False
          startTime = time.time()
        else:
          # Pretend query was issued back when we wanted it to be;
          # this way a system-wide hang is still "counted":
          startTime = targetTime
          if not warned and pause < -.005:
            out.write('WARNING: hiccup %.1f msec\n' % (-1000*pause))
            warned = True

        #origTask = task
        tasks.send(startTime, task)

      t = time.time()

      if doSweep:
        if t - lastSweepCheck > SWEEP_CHECK_EVERY_SEC:
          if meanQPS == SWEEP_START_QPS and len(tasks.sent) > 4:
            print 'Sweep: stay @ %s QPS for warmup...' % SWEEP_START_QPS
          elif len(tasks.sent) < 10000:
            # Still not saturated
            meanQPS *= 2
            print 'Sweep: set target to %.1f QPS' % meanQPS
          else:
            break
          lastSweepCheck = t
      elif t - tasks.startTime > runTimeSec:
        break
      elif iters == iteration:
        break

    print 'Sent all tasks %d times.' % iters

  except KeyboardInterrupt:
    if not handleCtrlC:
      raise
    # Ctrl-c to stop the test
    print
    print 'Ctrl+C: stopping now...'
    print

  out.write('%8.1f sec: Done sending tasks...\n' % (time.time()-tasks.startTime))
  out.flush()
  try:
    while len(tasks.sent) != 0 or tasks.results.nr_results != tasks.taskID:
      time.sleep(1)
  except KeyboardInterrupt:
    if not handleCtrlC:
      raise
    pass
  print "send %d tasks, receive %d results\n" %(tasks.taskID, tasks.results.nr_results)
  out.write('%8.1f sec: Done...\n' % (time.time()-tasks.startTime))
  out.flush()

  tasks.results.finish()

# printResults(tasks.results)

def printResults(results):
  for startTime, taskString, latencyMS, queueTimeMS in results:
    print '%8.3f sec: latency %8.1f msec; queue msec %.1f; task %s' % (startTime, latencyMS, queueTimeMS, taskString)

#../script/sendDupTasks.py ../script/20K.term.tasks td1 7777 1000 1000000 200000 abcd_1000_20 20 order 100
if __name__ == '__main__':
  tasksFile = sys.argv[1]
  serverHost = sys.argv[2]
  serverPort = int(sys.argv[3])
  s = sys.argv[4]
  if s == 'sweep':
    meanQPS = s
  else:
    meanQPS = float(s)
  numTasksPerCat = int(sys.argv[5])
  runTimeSec = float(sys.argv[6])
  savFile = sys.argv[7]
  iteration = sys.argv[8]
  randomshuffle = sys.argv[9]
  randombackup = int(sys.argv[10]);

  run(tasksFile, serverHost, serverPort, meanQPS, numTasksPerCat, runTimeSec, savFile, iteration,  sys.stdout, True, randomshuffle, randombackup)
