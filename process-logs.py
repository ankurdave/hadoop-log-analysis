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

class TaskInfo:
    def __init__(self, start_time, end_time, task_type, job_id):
        self.start_time = start_time
        self.end_time = end_time
        self.task_type = task_type
        self.job_id = job_id

def parseTime(time_string):
    return datetime.datetime.strptime(
        time_string + '000', '%Y-%m-%d %H:%M:%S,%f')

task_start_re = re.compile('^([0-9:, -]+) INFO org.apache.hadoop.mapred.JobTracker .*: Adding task \((.*)\) .* to tip ([^ ]*), for tracker')
def matchTaskStart(line, tasks, job_id):
    match = task_start_re.match(line)
    if match:
        task_id = match.group(3)
        task_start_time = parseTime(match.group(1))
        task_info = TaskInfo(task_start_time, None, match.group(2), job_id)
        tasks[task_id] = task_info

task_end_re = re.compile('^([0-9:, -]+) INFO org.apache.hadoop.mapred.JobInProgress .*: Task .* has completed ([^ ]*) successfully.$')
def matchTaskEnd(line, tasks):
    match = task_end_re.match(line)
    if match:
        task_end_time = parseTime(match.group(1))
        task_id = match.group(2)
        if task_id in tasks:
            task_info = tasks[task_id]
            task_info.end_time = task_end_time
            tasks[task_id] = task_info

class JobInfo:
    def __init__(self, start_time, end_time, map_capacity, reduce_capacity):
        self.start_time = start_time
        self.end_time = end_time
        self.map_capacity = map_capacity
        self.reduce_capacity = reduce_capacity

job_start_re = re.compile('INFO org.apache.hadoop.mapred.JobInProgress .*: (.*): nMaps=')
def matchJobStart(line):
    match = job_start_re.search(line)
    if match:
        job_id = match.group(1)
        return job_id
    else:
        return None

job_end_re = re.compile('INFO org.apache.hadoop.mapred.JobInProgress\$JobSummary .*: jobId=([^,]+),.*launchTime=(\d+),.*finishTime=(\d+),.*clusterMapCapacity=(\d+),clusterReduceCapacity=(\d+)')
def matchJobEnd(line, jobs):
    match = job_end_re.search(line)
    if match:
        job_id = match.group(1)
        job_info = JobInfo(datetime.datetime.utcfromtimestamp(int(match.group(2)) / 1000),
                           datetime.datetime.utcfromtimestamp(int(match.group(3)) / 1000),
                           int(match.group(4)),
                           int(match.group(5)))
        jobs[job_id] = job_info

def findTasksInFile(path):
    with open(path, 'r') as f:
        jobs = {}
        tasks = {}
        job_id = None
        for line in f:
            maybe_job_id = matchJobStart(line)
            if maybe_job_id:
                job_id = maybe_job_id
            matchJobEnd(line, jobs)
            matchTaskStart(line, tasks, job_id)
            matchTaskEnd(line, tasks)
        return jobs, tasks

# Load start and end times of Hadoop tasks
jobs = {}
tasks = {}
for root, dirnames, filenames in os.walk(sys.argv[1]):
    for filename in fnmatch.filter(filenames, 'hadoop-hadoop-jobtracker-*.log*'):
        file_jobs, file_tasks = findTasksInFile(os.path.join(root, filename))
        jobs.update(file_jobs)
        tasks.update(file_tasks)

def formatTime(t):
    if t:
        return t.strftime('%Y-%m-%d %H:%M:%S,%f')
    else:
        return 'None'

for job_id, job_info in sorted(jobs.items()):
    print '%s: %s - %s' % (
        job_id,
        formatTime(job_info.start_time),
        formatTime(job_info.end_time))

for task_id, task_info in sorted(tasks.items()):
    print '%s/%s (%s): %s - %s' % (
        task_info.job_id, task_id, task_info.task_type,
        formatTime(task_info.start_time),
        formatTime(task_info.end_time))

def summarize(nums):
    v = sorted(nums)
    if v:
        return (len(v),
                v[0],
                v[int(0.5 * len(v))],
                v[int(0.99 * len(v))],
                v[int(len(v) - 1)])
    else:
        return (0,0,0,0,0)

print 'Job duration: n %d, min %.2f, tp50 %.2f, tp99 %.2f, max %.2f' % summarize([
    (job_info.end_time - job_info.start_time).total_seconds()
    for job_info in jobs.values()
    if job_info.start_time and job_info.end_time])

print 'Task duration: n %d, min %.2f, tp50 %.2f, tp99 %.2f, max %.2f' % summarize([
    (task_info.end_time - task_info.start_time).total_seconds()
    for task_info in tasks.values()
    if task_info.start_time and task_info.end_time])

for job_id, job_info in sorted(jobs.items()):
    total_task_durations = sum([(task_info.end_time - task_info.start_time).total_seconds()
                                for task_info in tasks.values()
                                if task_info.job_id == job_id])
    job_duration = (job_info.end_time - job_info.start_time).total_seconds()
    # TODO: Split into two phases because map slots and reduce slots overlap
    total_slots = job_info.map_capacity + job_info.reduce_capacity
    print '%s: utilization %.1f%%' % (job_id, total_task_durations / (job_duration * total_slots) * 100)
