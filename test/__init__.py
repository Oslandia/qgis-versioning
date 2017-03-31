# coding = utf-8

import os
import sys
from subprocess import Popen, PIPE


def test():
    __current_dir = os.path.abspath(os.path.dirname(__file__))

    tests = ["qgis_versioning.test."+file_[:-3]
        for file_ in os.listdir(__current_dir)
        if file_[-8:]=="_test.py"]

    failed = 0
    for i, test in enumerate(tests):
        sys.stdout.write("% 4d/%d %s %s"%(
            i+1, len(tests), test, "."*max(0, (80-len(test)))))
        sys.stdout.flush()
        child = Popen(["python", "-m", test], stdout=PIPE, stderr=PIPE)
        out, err = child.communicate()
        if child.returncode:
            sys.stdout.write("failed\n")
            if len(sys.argv) == 2 and sys.argv[1] == '-v':
                sys.stdout.write(err)
            failed += 1
        else:
            sys.stdout.write("ok\n")

    if failed:
        sys.stderr.write("%d/%d test failed\n"%(failed, len(tests)))
        raise RuntimeError
    else:
        sys.stdout.write("%d/%d test passed (%d%%)\n"%(
            len(tests)-failed,
            len(tests),
            int((100.*(len(tests)-failed))/len(tests))))
