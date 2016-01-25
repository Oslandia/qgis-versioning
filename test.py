#!/usr/bin/python
# coding=UTF-8

import os
import sys
from subprocess import Popen, PIPE

__current_dir = os.path.dirname(__file__)
tests = []
test_directories = ["test"]
for dir_ in test_directories:
    test_dir = os.path.join(__current_dir, dir_)
    for file_ in os.listdir(test_dir):
        if file_[-8:]=="_test.py":
            tests.append(os.path.join(test_dir, file_))

failed = 0
for i, test in enumerate(tests):
    sys.stdout.write("% 4d/%d %s %s"%(
        i+1, len(tests), test, "."*max(0, (80-len(test)))))
    sys.stdout.flush()
    child = Popen(["python", test], stdout=PIPE, stderr=PIPE)
    out, err = child.communicate()
    if child.returncode:
        sys.stdout.write("failed\n")
        failed += 1
    else:
        sys.stdout.write("ok\n")

if failed:
    sys.stderr.write("%d/%d test failed\n"%(failed, len(tests)))
    exit(1)
else:
    sys.stdout.write("%d/%d test passed (%d%%)\n"%(
        len(tests)-failed,
        len(tests),
        int((100.*(len(tests)-failed))/len(tests))))

exit(0)
