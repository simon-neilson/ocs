#!/usr/bin/python

import sys
import re
import gzip
import subprocess
import os
import cStringIO
    
pattern = '^.*?\,143,4605,3\>.*?\,330,4605,3\>\[\d*\,\d*\,\d*\,(.*?)\,.*?\,330,4605,3\>\[\d*\,\d*\,\d*\,(.*?)\,.*?\,330,4605,3\>\[\d*\,\d*\,\d*\,(.*?)\,.*?\,625,4605,3\>\[.*?\,.*?\,(.*?)\,.*\}\,\d\d\d\d\-\d\d\-\d\d.*?\,(.*?)\,.*?\,-10,4605,3\>\[(\d+).*?\,646\,4605\,3\>.*?\:(.*?)\)\,(.*?)[\,|\]]'
io_method = cStringIO.StringIO

print 'Number of arguments:', len(sys.argv), 'arguments.'
print 'Argument List:', str(sys.argv)


p = subprocess.Popen(["gzcat", sys.argv[1]], stdout = subprocess.PIPE)
fh = io_method(p.communicate()[0])
assert p.returncode == 0

prog = re.compile(pattern)

hit = 0
miss = 0

for line in fh:
	m = prog.match(line) 
	if m is not None:
		print m.group(4) + ', ' + m.group(7) + ', ' + m.group(8) + ', ' + m.group(5) + ', ' + m.group(6) + ', ' + m.group(1) + ', ' + m.group(2) + ', ' + m.group(3)
		hit = hit + 1
        else:
		miss = miss + 1

print 'Hit count = ' + str(hit) + ', Miss count = ' + str(miss)
