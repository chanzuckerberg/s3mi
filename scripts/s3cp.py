#!/usr/bin/env python

from subprocess import check_call

import sys
import os
import traceback

def safe_remove(f):
    try:
        if os.path.exists(f):
            os.remove(f)
    except:
        traceback.print_exc()
        pass


def tsprint(msg):
    sys.stderr.write(msg)
    sys.stderr.write("\n")


if __name__ == "__main__":
    tsprint("WARNING:  The s3cp.py script is deprecated.  Use 's3mi cp' or 's3mi cat' instead.")
    if sys.argv[2] == "-":
        check_call(["s3mi", "cat", sys.argv[1]])
    else:
        safe_remove(sys.argv[2])
        with open(sys.argv[2], "ab") as dest:
            check_call(["s3mi", "cat", sys.argv[1]], stdout=dest)
