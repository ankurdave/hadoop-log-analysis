#!/usr/bin/env python2.7

import datetime
import fnmatch
import os
import re
import sys
import xml.etree.ElementTree

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
    def __init__(self, start_time, first_map_launch_time, first_reduce_launch_time, end_time, map_capacity, reduce_capacity):
        self.start_time = start_time
        self.first_map_launch_time = first_map_launch_time
        self.first_reduce_launch_time = first_reduce_launch_time
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

job_end_re = re.compile('INFO org.apache.hadoop.mapred.JobInProgress\$JobSummary .*: jobId=([^,]+),.*launchTime=(\d+),firstMapTaskLaunchTime=(\d+),firstReduceTaskLaunchTime=(\d+),.*finishTime=(\d+),.*clusterMapCapacity=(\d+),clusterReduceCapacity=(\d+)')
def matchJobEnd(line, jobs):
    match = job_end_re.search(line)
    if match:
        job_id = match.group(1)
        job_info = JobInfo(start_time=datetime.datetime.utcfromtimestamp(int(match.group(2)) / 1000),
                           first_map_launch_time=datetime.datetime.utcfromtimestamp(int(match.group(3)) / 1000),
                           first_reduce_launch_time=datetime.datetime.utcfromtimestamp(int(match.group(4)) / 1000),
                           end_time=datetime.datetime.utcfromtimestamp(int(match.group(5)) / 1000),
                           map_capacity=int(match.group(6)),
                           reduce_capacity=int(match.group(7)))
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

def readJobConf(conf_file, jobs):
    conf = {}
    for prop in xml.etree.ElementTree.parse(conf_file).getroot().iter('property'):
        conf[prop.find('name').text] = prop.find('value').text
    return conf

def lookup(d, ks):
    for k in ks:
        if k in d:
            return d[k]
    return None

# Load start and end times of Hadoop tasks
jobs = {}
tasks = {}
job_confs = {}
for root, dirnames, filenames in os.walk(sys.argv[1]):
    for filename in fnmatch.filter(filenames, 'hadoop-hadoop-jobtracker-*.log*'):
        file_jobs, file_tasks = findTasksInFile(os.path.join(root, filename))
        jobs.update(file_jobs)
        tasks.update(file_tasks)
    for filename in fnmatch.filter(filenames, 'job_*_conf.xml'):
        match = re.match('(job_.*)_conf.xml', filename)
        if match:
            job_id = match.group(1)
            conf = readJobConf(os.path.join(root, filename), jobs)
            job_confs[job_id] = conf

def formatTime(t):
    if t:
        return t.strftime('%Y-%m-%d %H:%M:%S,%f')
    else:
        return 'None'

# for job_id, job_info in sorted(jobs.items()):
#     print '%s: %s - %s' % (
#         job_id,
#         formatTime(job_info.start_time),
#         formatTime(job_info.end_time))

# for task_id, task_info in sorted(tasks.items()):
#     print '%s/%s (%s): %s - %s' % (
#         task_info.job_id, task_id, task_info.task_type,
#         formatTime(task_info.start_time),
#         formatTime(task_info.end_time))

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

def contains(task_info, time):
    return task_info.start_time <= time and task_info.end_time >= time

# TODO: handle adjacent but not overlapping
def overlap(t1, t2):
    def overlap_helper(t1, t2):
        return contains(t1, t2.start_time) or contains(t1, t2.end_time)
    return overlap_helper(t1, t2) or overlap_helper(t2, t1)

for job_id, job_info in sorted(jobs.items()):
    tasks_for_job = [task_info
                     for task_info in tasks.values()
                     if task_info.job_id == job_id
                     and task_info.start_time and task_info.end_time]
    map_tasks = [task_info
                 for task_info in tasks_for_job
                 if task_info.task_type == 'MAP']
    reduce_tasks = [task_info
                    for task_info in tasks_for_job
                    if task_info.task_type == 'REDUCE']
    total_m_task_durations = sum([
        (task_info.end_time - task_info.start_time).total_seconds()
        for task_info in map_tasks])
    total_r_task_durations = sum([
        (task_info.end_time - task_info.start_time).total_seconds()
        for task_info in reduce_tasks])
    if map_tasks:
        first_m_task_launched = min(
            [task_info.start_time for task_info in map_tasks])
        last_m_task_finished = max(
            [task_info.end_time for task_info in map_tasks])
        m_time = (last_m_task_finished - first_m_task_launched).total_seconds()
    if reduce_tasks:
        first_r_task_launched = min(
            [task_info.start_time for task_info in reduce_tasks])
        last_r_task_finished = max(
            [task_info.end_time for task_info in reduce_tasks])
        r_time = (last_r_task_finished - first_r_task_launched).total_seconds()
    if map_tasks and reduce_tasks:
        m_utilization = (
            total_m_task_durations / (m_time * job_info.map_capacity) * 100)
        r_utilization = (
            total_r_task_durations / (r_time * job_info.reduce_capacity) * 100)
        task_stats = summarize([(task_info.end_time - task_info.start_time).total_seconds()
                                for task_info in map_tasks])

        print '%s: m utilization %.1f%%, r utilization %.1f%%, tasks [n %d, min %.2f, tp50 %.2f, tp99 %.2f, max %.2f]' % (
            (job_id, m_utilization, r_utilization) + task_stats)
        num_stragglers = 0
        wasted_time = 0
        # Check for stragglers (tasks that continue to run after all
        # simultaneous tasks have finished)
        for t1 in map_tasks:
            simultaneous_tasks = filter(
                lambda t2: t1 is not t2 and overlap(t1, t2), map_tasks)
            if simultaneous_tasks:
                last_simultaneous_task_end = max([t2.end_time for t2 in simultaneous_tasks])
                if last_simultaneous_task_end < t1.end_time:
                    num_stragglers += 1
                    wasted_time += (t1.end_time - last_simultaneous_task_end).total_seconds()
        job_duration = (job_info.end_time - job_info.start_time).total_seconds()
        wasted_time_fraction = wasted_time / job_duration
        if wasted_time_fraction > 0.3:
            print '    # stragglers %d, wasted time %.2f, wasted time fraction %.1f%%' % (
                 num_stragglers, wasted_time, wasted_time_fraction * 100)
            if job_id in job_confs:
                print '    %s, %s' % (
                    lookup(conf, ['mapred.map.runner.class',
                                  'mapred.mapper.class',
                                  'mapreduce.map.class']),
                    lookup(conf, ['mapred.reducer.class']))

    else:
        print '%s empty' % job_id
