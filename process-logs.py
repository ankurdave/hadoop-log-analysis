#!/usr/bin/env python2.7

import datetime
import fnmatch
import os
import re
import sys

if len(sys.argv) < 2:
    print """Usage: process-logs.py <log_dir>"""
    sys.exit(1)

if not os.path.isdir(sys.argv[1]):
    print "Not a directory: %s" % sys.argv[1]
    sys.exit(1)

task_start_re = re.compile('^([0-9:, -]+) INFO org.apache.hadoop.mapred.JobTracker .*: Adding task \((.*)\) .* to tip ([^ ]*), for tracker')
task_end_re = re.compile('^([0-9:, -]+) INFO org.apache.hadoop.mapred.JobInProgress .*: Task .* has completed ([^ ]*) successfully.$')

class TaskInfo:
    start_time = None
    end_time = None
    task_type = None
    def __init__(self, start_time, end_time, task_type):
        self.start_time = start_time
        self.end_time = end_time
        self.task_type = task_type

def parseTime(time_string):
    return datetime.datetime.strptime(
        time_string + '000', '%Y-%m-%d %H:%M:%S,%f')

def matchTaskStart(line, tasks):
    match = task_start_re.match(line)
    if match:
        task_id = match.group(3)
        task_start_time = parseTime(match.group(1))
        task_info = TaskInfo(task_start_time, None, match.group(2))
        tasks[task_id] = task_info

def matchTaskEnd(line, tasks):
    match = task_end_re.match(line)
    if match:
        task_end_time = parseTime(match.group(1))
        task_id = match.group(2)
        if task_id in tasks:
            task_info = tasks[task_id]
            task_info.end_time = task_end_time
            tasks[task_id] = task_info

def findTasksInFile(path):
    with open(path, 'r') as f:
        tasks = {}
        for line in f:
            matchTaskStart(line, tasks)
            matchTaskEnd(line, tasks)
        return tasks

# Load start and end times of Hadoop tasks
tasks = {}
for root, dirnames, filenames in os.walk(sys.argv[1]):
    for filename in fnmatch.filter(filenames, 'hadoop-hadoop-jobtracker-*.log*'):
        file_tasks = findTasksInFile(os.path.join(root, filename))
        tasks.update(file_tasks)

def formatTime(t):
    if t:
        return t.strftime('%Y-%m-%d %H:%M:%S,%f')
    else:
        return 'None'

for task_id, task_info in sorted(tasks.items()):
    print '%s (%s): %s - %s' % (
        task_id, task_info.task_type,
        formatTime(task_info.start_time),
        formatTime(task_info.end_time))

def summarize(nums):
    v = sorted(nums)
    return (v[0],
            v[int(0.5 * len(v))],
            v[int(0.99 * len(v))],
            v[int(len(v) - 1)])

print 'Task duration: min %.2f, tp50 %.2f, tp99 %.2f, max %.2f' % summarize([
    (task_info.end_time - task_info.start_time).total_seconds()
    for task_info in tasks.values()
    if task_info.start_time and task_info.end_time])
