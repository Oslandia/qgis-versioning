# -*- coding: utf-8 -*-

# coding = utf-8

import os
import sys

from subprocess import Popen, PIPE


def test(host, pguser, verbose=True):
    __current_dir = os.path.abspath(os.path.dirname(__file__))
    plugin_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    tests = [os.path.join(__current_dir,file_)
        for file_ in os.listdir(__current_dir)
        if file_[-8:]=="_test.py"]

    failed = 0
    env = os.environ.copy()
    env['PYTHONPATH'] = plugin_dir + (":" + env['PYTHONPATH'] if 'PYTHONPATH'
                                      in env else "")
    for i, test in enumerate(tests):
        sys.stdout.write("% 4d/%d %s %s"%(
            i+1, len(tests), test, "."*max(0, (80-len(test)))))
        sys.stdout.flush()
        child = Popen([sys.executable, test, host, pguser], stdout=PIPE,
                      stderr=PIPE, env=env)
        out, err = child.communicate()
        if child.returncode:
            sys.stdout.write("failed\n")
            if verbose:
                sys.stdout.write(err.decode("utf-8"))
            failed += 1
        else:
            sys.stdout.write("ok\n")

    if failed:
        sys.stderr.write("%d/%d test failed\n"%(failed, len(tests)))
        raise RuntimeError("%d/%d test failed\n"%(failed, len(tests)))
    else:
        sys.stdout.write("%d/%d test passed (%d%%)\n"%(
            len(tests)-failed,
            len(tests),
            int((100.*(len(tests)-failed))/len(tests))))


if __name__=="__main__":
    if len(sys.argv) <= 2 or len(sys.argv) > 4:
        print("Usage: python3 tests.py HOST PGUSER [-v]")
        exit(0)
        
    verbose = False
    if len(sys.argv) == 4 and sys.argv[3] == '-v':
        verbose = True
        
    test(sys.argv[1], sys.argv[2], verbose)
