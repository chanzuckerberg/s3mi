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
import multiprocessing
import subprocess
import os
import sys
import time

EXABYTE = 2**50

# The goal is to stream from S3 at 2GB/sec.
# As each request is limited to 1 Gbit/sec, we need 16+ concurrent requests.
MAX_CONCURRENT_REQUESTS = 36

# Max RAM required = MAX_PENDING_APPENDS * MAX_SEGMENT_SIZE
MAX_PENDING_APPENDS = 512
MAX_SEGMENT_SIZE = 128*1024*1024

FILE_BUFFER_SIZE = 256*1024*1024

# Max time in seconds without a single chunk completing its fetch.
TIMEOUT = 120

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

def concatenate_chunk(destination, segment_bytes):
    with open(destination, "ab", FILE_BUFFER_SIZE) as dest_file:
        dest_file.write(segment_bytes)

def stream_chunk(segment_bytes):
    sys.stdout.write(segment_bytes)

def set_state(status, n, state, lock):
    try:
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
    except PriorErrors:
        raise
    except:
        pass

def safe_remove(f):
    try:
        if os.path.exists(f):
            os.remove(f)
    except:
        pass

def fetch_chunk(s3_bucket, s3_key, destination, n, N, size, status, last_semaphore, next_semaphore, requests_semaphore, appends_semaphore, lock):
    try:
        released = False
        first = segment_start(n, N, size)
        last = segment_start(n + 1, N, size) - 1  # aws api wants inclusive bounds
        part = part_filename(destination, n, N)
        command_str = "aws s3api get-object --range bytes={rfrom}-{rto} --bucket {bucket} --key {key} {part}".format(
            rfrom=first, rto=last, bucket=s3_bucket, key=s3_key, part=part)
        set_state(status, n, 'removing temporary file', lock)
        safe_remove(part)
        os.mkfifo(part)
        set_state(status, n, 'fetching', lock)
        with open("/dev/null", "ab") as devnull:
            fetcher = subprocess.Popen(command_str.split(), stdout=devnull)
        with open(part, 'rb') as pf:
            segment_bytes = pf.read()
        if fetcher.wait() != 0:
            raise CalledProcessError
        released = True
        requests_semaphore.release()
        safe_remove(part)
        if last_semaphore:
            set_state(status, n, "waiting for part {previous} to finish".format(previous=(n - 1)), lock)
            last_semaphore.acquire()
        set_state(status, n, 'concatenating', lock)
        if destination == "-":  # streaming mode
            stream_chunk(segment_bytes)
        else:
            concatenate_chunk(destination, segment_bytes)
        set_state(status, n, 'succeeded', lock)
    except PriorErrors:
        pass
    except:
        set_state(status, n, 'failed', lock)
    finally:
        if not released:
            requests_semaphore.release()
            safe_remove(part)
        appends_semaphore.release()
        next_semaphore.release()

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
    status = multiprocessing.Manager().dict()
    lock = multiprocessing.RLock()
    requests_semaphore = multiprocessing.Semaphore(MAX_CONCURRENT_REQUESTS)
    appends_semaphore = multiprocessing.Semaphore(MAX_PENDING_APPENDS)
    last_semaphore = None
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
        while not error and not timeout and not requests_semaphore.acquire(block=True, timeout=1.0):
            timeout = (time.time() - t0) > TIMEOUT
            with lock:
                error = (status.get(-1) != None)
        if timeout:
            tsprint("Exceeded timeout {} seconds.".format(TIMEOUT))
        if error or timeout:
            break
        while not error and not timeout and not appends_semaphore.acquire(block=True, timeout=1.0):
            timeout = (time.time() - t0) > TIMEOUT
            with lock:
                error = (status.get(-1) != None)
        if timeout:
            tsprint("Exceeded timeout {} seconds.".format(TIMEOUT))
        if error or timeout:
            break
        next_semaphore = multiprocessing.Semaphore(1)
        next_semaphore.acquire()
        multiprocessing.Process(
            target=fetch_chunk,
            args=[s3_bucket, s3_key, destination_download, n, N, file_size, status, last_semaphore, next_semaphore, requests_semaphore, appends_semaphore, lock]
        ).start()
        last_semaphore = next_semaphore
    # each process waits for the previous one, so joining the last joins all
    last_semaphore.acquire()
    if len(status) == N and all(s == "succeeded" for s in status.values()):
        if destination != "-":
            os.rename(destination_download, destination)
        tsprint("Great success.")
    else:
        # tsprint(str(sorted(status.items())))
        if destination != "-":
            safe_remove(destination_download)
        tsprint("There were some failures, sorry.")
        sys.exit(-1)

if __name__ == "__main__":
    try:
        main(s3_uri=sys.argv[1],
             destination=(len(sys.argv) > 2 and sys.argv[2]) or "-")
    except KeyboardInterrupt:
        pass
    except:
        tsprint(help_text)
        raise
