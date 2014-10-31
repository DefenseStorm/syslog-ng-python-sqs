#!/usr/bin/env python
from __future__ import with_statement
from ConfigParser import SafeConfigParser
from collections import deque
from threading import Timer, Lock
import json, time
import boto.sqs as sqs

local_queue = sqs_queue = timer = None
# Deadlock semantics: NEVER take both locks on the same thread
timer_lock = Lock() # Guards all changes to `timer`
flush_lock = Lock() # Guards consumption of `local_queue`

def reset_timer(seconds):
    global timer, timer_lock
    with timer_lock:
        if timer != None:
            timer.cancel()
        timer = Timer(seconds, flush_queue)

def start_timer():
    global timer, timer_lock
    with timer_lock:
        if timer == None:
            timer = Timer(flush_seconds, flush_queue)

def flush_queue(force=False):
    global local_queue, sqs_queue, flush_lock
    try:
        while (force and len(local_queue) > 0) or len(local_queue) >= flush_lines \
                or time.time() - local_queue[0][0] >= flush_seconds:
            with flush_lock:
                # Break if another thread flushed enough while this one waited for the lock
                if not (force and len(local_queue) > 0) and len(local_queue) < flush_lines \
                        and time.time() - local_queue[0][0] < flush_seconds:
                    break
                messages = []
                while len(messages) < flush_lines:
                    try:
                        messages.append(local_queue.popleft()[1])
                    except IndexError:
                        break
                groups = [messages[i::10] for i in range(10)]
                json_groups = [json.dumps(group) for group in groups if len(group) > 0]
                sqs_messages = [(i, json, 0) for i, json in enumerate(json_groups)]
                sqs_queue.write_batch(groups)
        reset_timer(local_queue[0][0] + flush_seconds - time.time())
    except IndexError:
        # If local_queue[0] throws, we're done flushing and don't need to set a timer
        pass

def queue(message):
    global local_queue
    local_queue.append((time.time(), message))
    if len(local_queue) >= flush_lines:
        reset_timer(0)
    else:
        start_timer()

def init():
    global local_queue, sqs_queue
    config = SafeConfigParser()
    config.read('/etc/syslog-ng/python_sqs.conf')

    flush_seconds = config.getfloat("Flush", "Seconds")
    flush_lines = config.getfloat("Flush", "Lines")

    local_queue = deque()

    conn = sqs.connect_to_region(
        config.get("AWS", "Region"),
        aws_access_key_id=config.get("AWS", "AccessKeyID"),
        aws_secret_access_key=config.get("AWS", "SecretAccessKey"))
    sqs_queue = conn.get_queue("AWS", "SQSQueueName")

def deinit():
    global timer, timer_lock, local_queue, sqs_queue
    with timer_lock:
        if timer != None:
            timer.cancel()
        timer = None
    flush_queue(True)
    local_queue = sqs_queue = None
