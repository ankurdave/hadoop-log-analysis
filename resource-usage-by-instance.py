#!/usr/bin/env python2.7

import fnmatch
import itertools
import os
import re
import shelve
import subprocess
import sys

if len(sys.argv) < 2:
    print """Usage: resource-usage-by-instance.py <log_dir>"""
    sys.exit(1)

if not os.path.isdir(sys.argv[1]):
    print "Not a directory: %s" % sys.argv[1]
    sys.exit(1)

class ResourceUtilization:
    def __init__(self):
        pass
def parseSize(s):
    if s.endswith('G'):
        return float(s[0:len(s)-1]) * 1024 * 1024 * 1024
    if s.endswith('M'):
        return float(s[0:len(s)-1]) * 1024 * 1024
    if s.endswith('K'):
        return float(s[0:len(s)-1]) * 1024
    return float(s)

def avg(l):
    return sum(l) / len(l)

def readInstanceState(state_file):
    cpu_usage = []
    mem_usage = 0
    mem_total = 0
    disk_totals = []
    disk_usage = []
    with open(state_file, 'r') as f:
        in_process_list = False
        next_line_is_cpu = False
        next_lines_are_disk = False
        next_line_is_disk_cont = False
        for line in f:
            if line == '# whats io usage look like\n':
                in_process_list = True
            if line == '# whats using the disk\n':
                in_process_list = False

            if in_process_list:
                if line.startswith('avg-cpu:'):
                    next_line_is_cpu = True
                elif next_line_is_cpu:
                    next_line_is_cpu = False
                    idle_pct = float(line.split()[5])
                    cpu_usage += [100 - idle_pct]

                if line.startswith('Mem:'):
                    mem_total = int(line.split()[1])
                    mem_usage = int(line.split()[2])

                if next_lines_are_disk and line == '\n':
                    next_lines_are_disk = False

                if line.startswith('Filesystem'):
                    next_lines_are_disk = True
                elif next_lines_are_disk:
                    cols = line.split()
                    if len(cols) <= 1:
                        next_line_is_disk_cont = True
                    elif next_line_is_disk_cont and len(cols) >= 2:
                        next_line_is_disk_cont = False
                        disk_totals += [parseSize(cols[0])]
                        disk_usage += [parseSize(cols[1])]
                    elif len(cols) >= 2:
                        disk_totals += [parseSize(cols[1])]
                        disk_usage += [parseSize(cols[2])]

    util = ResourceUtilization()
    util.cpu_usage = cpu_usage
    util.mem_usage = mem_usage
    util.mem_total = mem_total
    util.disk_total = sum(disk_totals)
    util.disk_usage = sum(disk_usage)

    return util

cache = shelve.open('job_flow_id_and_instance_type_cache')
def extractIds(path):
    match = re.search('(j-[^/]+)/node/(i-[^/]+)/daemons', path)
    if match:
        node_id = match.group(2)
        enc_job_flow_id = match.group(1)
        if cache.has_key(enc_job_flow_id):
            job_flow_id = cache[enc_job_flow_id]
        else:
            job_flow_id = subprocess.check_output([
                '/rhel5pdi/apollo/package/local_1/Linux-2.6c2.5-x86_64/JDK/JDK-4674.0-0/jdk1.7.0/bin/java',
                '-jar', '/workplace/ankudave/Aws157LogAnalyzer/build/Aws157LogAnalyzer_ankudave/Aws157LogAnalyzer_ankudave-1.0/RHEL5_64/DEV.STD.PTHREAD/build/artifacts/Aws157LogAnalyzer/lib/Aws157LogAnalyzer.jar',
                enc_job_flow_id])
            cache[enc_job_flow_id] = job_flow_id
        return (enc_job_flow_id, long(job_flow_id), node_id)
    else:
        raise Exception('Path %s malformed' % root)

def getInstanceType(jf_id, node_id):
    if cache.has_key('%s/%s' % (jf_id, node_id)):
        return cache['%s/%s' % (jf_id, node_id)]
    else:
        output = subprocess.check_output([
            'ssh', 'aws157-prod-na-db-6001.iad6.amazon.com',
            """/apollo/env/Aws157Database/bin/mysql-client -D Aws157 -e "SELECT instanceType FROM JobFlowInstance I, JobFlowInstanceGroup IG WHERE I.jobFlowId = %s AND I.jobFlowInstanceGroupId = IG.jobFlowInstanceGroupId AND I.instanceId = '%s';" --skip-column-names -B"""
            % (jf_id, node_id)], stderr=subprocess.STDOUT)
        instance_type = output.split()[-1]
        cache['%s/%s' % (jf_id, node_id)] = instance_type
        return instance_type

utilization_by_instance_type = {}
for root, dirnames, filenames in os.walk(sys.argv[1]):
    for filename in fnmatch.filter(filenames, 'instance-state.log*'):
        enc_jf_id, job_flow_id, node_id = extractIds(root)
        instance_type = getInstanceType(job_flow_id, node_id)
        print '%s (%s=%s - %s)' % (instance_type, enc_jf_id, job_flow_id, node_id)
        u = readInstanceState(os.path.join(root, filename))
        print 'CPU: %s' % u.cpu_usage
        print 'Mem usage: %s' % u.mem_usage
        print 'Mem total: %s' % u.mem_total
        print 'Disk usage: %s' % u.disk_usage
        print 'Disk total: %s' % u.disk_total
        print ''
        sys.stdout.flush()

        if instance_type not in utilization_by_instance_type:
            utilization_by_instance_type[instance_type] = [u]
        else:
            utilization_by_instance_type[instance_type] += [u]

cache.close()

print '------'

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

def summarizeStr(nums):
    return 'min %.2f, tp50 %.2f, tp99 %.2f, max %.2f' % summarize(nums)[1:]

for instance_type, us in sorted(utilization_by_instance_type.items()):
    print '%s (n %d):' % (instance_type, len(us))
    print "\tCPU: %s" % summarizeStr(itertools.chain.from_iterable(
        [u.cpu_usage for u in us]))
    print "\tMem usage: %s" % summarizeStr([u.mem_usage for u in us])
    print "\tMem total: %s" % summarizeStr([u.mem_total for u in us])
    print "\tDisk usage: %s" % summarizeStr([u.disk_usage for u in us])
    print "\tDisk total: %s" % summarizeStr([u.disk_total for u in us])
    print ''
