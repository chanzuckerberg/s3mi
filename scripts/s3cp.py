#!/usr/bin/env python
#
# Tested under python 2.7, 3.5, 3.6.
#
# Copyright (c) 2017 Chan Zuckerberg Initiative, see LICENSE.

help_text="""
S3MI tools s3cp

Usage similar to "aws s3 cp", but 10x faster for large files (10+ GB)
on machines that have 10+ Gbps connection to S3.

Examples:

    # Copy from S3 to local file
    s3cp.py s3://my_bucket/my_big_file /mnt/instance_local_path/my_big_file

    # Stream from S3 through tar
    s3cp.py s3://my_bucket/my_big_file.tar.bz2 - | lbzip2 -dc | tar xvf -

License:
    https://github.com/chanzuckerberg/s3mi/blob/master/LICENSE
"""

import threading
import subprocess
import os
import sys
import time

EXABYTE = 2**50

# The goal is to stream from S3 at 2GB/sec.
# As each request is limited to 1 Gbit/sec, we need 16+ concurrent requests.
MAX_CONCURRENT_REQUESTS = 128

# The segmnets in flight should fit in the filesystem cache, so this puts an
# upper limit on the product of num_conc_req x seg_size.  We want segments as
# big as possible within that limit to reduce TCP/IP and DNS overhead for
# establishing each connection.
MAX_SEGMENT_SIZE = 128*1024*1024

FILE_BUFFER_SIZE = 256*1024*1024

# Max time in seconds without a single chunk completing its fetch.
TIMEOUT = 60

def tsprint(msg):
    sys.stderr.write(msg)
    sys.stderr.write("\n")

def num_segments(file_size):
    return (file_size + MAX_SEGMENT_SIZE - 1) // MAX_SEGMENT_SIZE

def segment_start(n, N, size):
    assert size < EXABYTE, "Sizes over an exabyte get squished by conversion to 64-bit float."
    return int(size * (float(n) / float(N)))

def part_filename(destination, n, N):
    return "part.{N}.{n:06d}.{destination}".format(destination=destination, N=N, n=n)

class PriorErrors(Exception):
    pass

def concatenate_chunk(destination, part):
    with open(part, "rb", FILE_BUFFER_SIZE) as cat_stdin:
        with open(destination, "ab", FILE_BUFFER_SIZE) as cat_stdout:
            subprocess.check_call("/bin/cat", stdin=cat_stdin, stdout=cat_stdout)

def stream_chunk(part):
    with open(part, "rb", FILE_BUFFER_SIZE) as cat_stdin:
        subprocess.check_call("/bin/cat", stdin=cat_stdin, stdout=sys.stdout)

def set_state(status, n, state, lock):
    with lock:
        last_state = status.get(n, 'lost in the weeds')
        if state != 'failed' and status.get(-1) != None:
            if state != 'succeeded':
                tsprint("Terminating part {n} after {state} due to erorr in part {failed_n}.".format(
                    n=n, state=last_state, failed_n=status[-1]))
            raise PriorErrors()
        if state == 'failed' and status.get(-1) == None:
            tsprint("Part {n} failed after {state}.".format(n=n, state=last_state))
            status[-1] = n
        status[n] = state

def safe_remove(f):
    try:
        if os.path.exists(f):
            os.remove(f)
    except:
        pass

def fetch_chunk(s3_bucket, s3_key, destination, n, N, size, status, previous_thread, semaphore, lock):
    try:
        first = segment_start(n, N, size)
        last = segment_start(n + 1, N, size) - 1  # aws api wants inclusive bounds
        part = part_filename(destination, n, N)
        command_str = "aws s3api get-object --range bytes={rfrom}-{rto} --bucket {bucket} --key {key} {part}".format(
            rfrom=first, rto=last, bucket=s3_bucket, key=s3_key, part=part)
        set_state(status, n, 'removing temporary file', lock)
        safe_remove(part)
        set_state(status, n, 'fetching', lock)
        subprocess.check_output(command_str.split())  # the output is brief, just a status
        if previous_thread:
            set_state(status, n, "waiting for part {previous} to finish".format(previous=(n - 1)), lock)
            previous_thread.join()
        set_state(status, n, 'concatenating', lock)
        if destination == "-":  # streaming mode
            stream_chunk(part)
        else:
            if n == 0:
                os.rename(part, destination)
            else:
                concatenate_chunk(destination, part)
        set_state(status, n, 'succeeded', lock)
    except PriorErrors:
        pass
    except:
        set_state(status, n, 'failed', lock)
        raise
    finally:
        semaphore.release()
        threading.Thread(target=safe_remove, args=[part]).start()

def get_file_size(s3_uri):
    command_str = "aws s3 ls {s3_uri}".format(s3_uri=s3_uri)
    tsprint(command_str)
    result = subprocess.check_output(command_str.split())
    return int(result.split()[2])

def s3_bucket_and_key(s3_uri):
    prefix = "s3://"
    assert s3_uri.startswith(prefix)
    return s3_uri[len(prefix):].split("/", 1)

def main(s3_uri, destination):
    s3_uri = sys.argv[1]
    file_size = get_file_size(s3_uri)
    tsprint("File size is {:3.1f} GB ({} bytes).".format(file_size/(2**30), file_size))
    s3_bucket, s3_key = s3_bucket_and_key(s3_uri)
    status = {}
    lock = threading.RLock()
    semaphore = threading.Semaphore(MAX_CONCURRENT_REQUESTS)
    last_thread = None
    N = num_segments(file_size)
    tsprint("Fetching {} segments.".format(N))
    if destination == "-":
        sys.stdout = os.fdopen(sys.stdout.fileno(), 'ab', FILE_BUFFER_SIZE)
        destination_download = destination
    else:
        destination_download = destination + ".download"
        safe_remove(destination_download)
    for n in range(N):
        error = False
        timeout = False
        t0 = time.time()
        while not error and not timeout and not semaphore.acquire(blocking=False):
            time.sleep(0.05)
            timeout = (time.time() - t0) > TIMEOUT
            with lock:
                error = (status.get(-1) != None)
        if timeout:
            tsprint("Exceeded timeout {} seconds.".format(TIMEOUT))
        if error or timeout:
            break
        last_thread = threading.Thread(
            target=fetch_chunk,
            args=[s3_bucket, s3_key, destination_download, n, N, file_size, status, last_thread, semaphore, lock]
        )
        last_thread.start()
    # each thread waits for the previous one, so joining the last joins all
    last_thread.join()
    if len(status) == N and all(s == "succeeded" for s in status.values()):
        if destination != "-":
            os.rename(destination_download, destination)
        tsprint("Great success.")
    else:
        # tsprint(str(sorted(status.items())))
        tsprint("There were some failures, sorry.")
        sys.exit(-1)

if __name__ == "__main__":
    try:
        main(s3_uri=sys.argv[1],
             destination=(len(sys.argv) > 2 and sys.argv[2]) or "-")

    except:
        tsprint(help_text)
        raise
