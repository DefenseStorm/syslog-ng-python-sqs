#!/usr/bin/env python
#
# Copyright 2014 Praesidio Inc.
# Written by: Brandon Smith <brandon@praesid.io>
#
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License version 2 as published by the
# Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License version 2 for
# more details.
#
# You should have received a copy of the GNU General Public License version 2
# along with this program; see the file COPYING.  If not, see
# <https://gnu.org/licenses/old-licenses/gpl-2.0.html>.
#

from __future__ import with_statement
from threading import Timer, Lock
from collections import deque
import time

class TimeAndSizeFlushingQueue:

    def __init__(self, flush_fn=None, flush_seconds=None, flush_lines=None):
        self._timer = None
        # Deadlock semantics: NEVER take both locks on the same thread
        self._timer_lock = Lock() # Guards all changes to `timer`
        self._queue = deque()
        self._flush_lock = Lock() # Guards consumption of `self._queue`

        self._fn = flush_fn
        self._seconds = flush_seconds
        self._lines = flush_lines

    def __del__(self):
        self.close()

    def _reset_timer(self, seconds=None):
        """Stop any running flush timer and set one for `seconds` from now if set

        >>> class Timer():
        ...     cancelled = False
        ...     def cancel(self):
        ...         self.cancelled = True
        >>> orig_timer = Timer()
        >>> queue = TimeAndSizeFlushingQueue(flush_seconds=1)
        >>> queue._timer = orig_timer
        >>> queue._reset_timer(1)
        >>> queue._timer == orig_timer
        False
        >>> queue._timer
        <_Timer(Thread-..., started ...)>
        >>> queue._timer.cancel()
        >>> orig_timer.cancelled
        True
        >>> queue = TimeAndSizeFlushingQueue()
        >>> queue._reset_timer(1)
        >>> queue._timer
        <_Timer(Thread-..., started ...)>
        >>> queue._reset_timer()
        >>> queue._timer
        """
        with self._timer_lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
            if seconds is not None:
                self._timer = Timer(seconds, self._flush_queue)
                self._timer.start()

    def _start_timer(self):
        """Start a timer for self._seconds from now (if self._seconds is not None)

        >>> queue = TimeAndSizeFlushingQueue(flush_seconds=1)
        >>> queue._start_timer()
        >>> orig_timer = queue._timer
        >>> orig_timer
        <_Timer(Thread-..., started ...)>
        >>> queue._start_timer()
        >>> queue._timer == orig_timer
        True
        >>> queue._timer.cancel()
        """
        with self._timer_lock:
            if self._seconds is not None and self._timer is None:
                self._timer = Timer(self._seconds, self._flush_queue)
                self._timer.start()

    def _should_flush(self, force=False):
        """Returns true if _flush_queue should keep flushing

        >>> queue = TimeAndSizeFlushingQueue(flush_seconds=1, flush_lines=100)
        >>> queue._should_flush()
        False
        >>> queue._should_flush(True)
        False
        >>> queue._queue.appendleft((time.time(),))
        >>> queue._should_flush()
        False
        >>> queue._should_flush(True)
        True
        >>> queue._queue.appendleft((time.time() - 2,))
        >>> queue._should_flush()
        True
        >>> queue._should_flush(True)
        True
        >>> queue._queue.clear()
        >>> queue._queue.extend((time.time(),) for i in range(100))
        >>> queue._should_flush()
        True
        >>> queue._should_flush(True)
        True
        """
        try:
            return bool(self._queue and 
                    (force or len(self._queue) >= self._lines
                        or time.time() - self._queue[0][0] >= self._seconds))
        except IndexError:
            return False

    def _flush_queue(self, force=False):
        """Flushes messages to the specified function in batches.
        Continues flushing until self._should_flush returns false.
        Batches messages by self._lines if set.

        >>> msgs = None
        >>> def fn(messages):
        ...     global msgs
        ...     msgs = messages
        >>> queue = TimeAndSizeFlushingQueue(flush_fn=fn)
        >>> queue.queue("a message")
        >>> msgs
        ['a message']
        >>> queue.queue("1 message")
        >>> msgs
        ['1 message']
        >>> queue.queue("2 message")
        >>> msgs
        ['2 message']
        >>> queue = TimeAndSizeFlushingQueue(flush_fn=fn, flush_seconds=1, flush_lines=2)
        >>> queue._queue.append((time.time(), "1 message")) # Don't use .queue cuz timer
        >>> queue._queue.append((time.time(), "2 message")) # Don't use .queue cuz timer
        >>> queue._queue.append((time.time(), "3 message")) # Don't use .queue cuz timer
        >>> queue._flush_queue()
        >>> msgs
        ['1 message', '2 message']
        >>> timer = queue._timer
        >>> timer
        <_Timer(Thread-..., started ...)>
        >>> queue.close()
        >>> queue._timer
        >>> time.sleep(.01) # Yield so that the timer thread can change state
        >>> timer
        <_Timer(Thread-..., stopped ...)>
        >>> msgs
        ['3 message']
        """
        while self._should_flush(force):
            with self._flush_lock:
                # Break if another thread flushed enough while waiting for lock
                if not self._should_flush(force):
                    break
                messages = []
                while len(messages) < self._lines:
                    try:
                        messages.append(self._queue.popleft()[1])
                    except IndexError:
                        break
                if self._fn:
                    self._fn(messages)
        try:
            if self._seconds is None:
                seconds = None
            else:
                seconds = self._queue[0][0] + self._seconds - time.time()
        except IndexError:
            # If self._queue[0] throws, no timer needed, just clear it
            seconds = None
        self._reset_timer(seconds)

    def queue(self, message):
        """Flushes messages to the specified function in batches.
        Continues flushing until self._should_flush returns false.
        Batches messages by self._lines if set.

        >>> msgs = None
        >>> def fn(messages):
        ...     global msgs
        ...     msgs = messages
        >>> queue = TimeAndSizeFlushingQueue(flush_fn=fn)
        >>> queue.queue("a message")
        >>> msgs
        ['a message']
        >>> queue.close()
        >>> queue = TimeAndSizeFlushingQueue(flush_lines=1)
        >>> queue.queue("a message")
        >>> queue._timer
        <_Timer(Thread-..., started ...)>
        >>> time.sleep(.001)
        >>> queue._timer
        >>> queue._queue
        deque([])
        >>> queue.close()
        >>> queue = TimeAndSizeFlushingQueue(flush_seconds=1, flush_lines=2)
        >>> queue.queue("a message")
        >>> first_timer = queue._timer
        >>> first_timer
        <_Timer(Thread-..., started ...)>
        >>> queue.queue("a message")
        >>> first_timer == queue._timer
        False
        >>> time.sleep(.001)
        >>> queue._queue
        deque([])
        """
        if not self._lines:
            if self._fn:
                self._fn([message])
            return
        self._queue.append((time.time(), message))
        if len(self._queue) >= self._lines:
            self._reset_timer(0)
        else:
            self._start_timer()

    def close(self):
        """Flush any remaining values to _fn (if set) and cleanup the timer
        """
        with self._timer_lock:
            if self._timer is not None:
                self._timer.cancel()
            self._timer = None
        self._flush_queue(True)

_queue = None

def queue(message):
    global _queue
    _queue.queue(message)

def init():
    global _queue
    from ConfigParser import SafeConfigParser
    import sys
    import boto.sqs as sqs
    config = SafeConfigParser()
    config.read('/etc/syslog-ng/python_sqs.conf')

    conn = sqs.connect_to_region(
        config.get("AWS", "Region"),
        aws_access_key_id=config.get("AWS", "AccessKeyID"),
        aws_secret_access_key=config.get("AWS", "SecretAccessKey"))
    sqs_queue = conn.get_queue(config.get("AWS", "SQSQueueName"))

    flush_single = config.getboolean("Flush", "Single")
    def flush_fn(messages):
        import json
        if flush_single:
            groups = messages
        else:
            groups = [messages[i::10] for i in range(10)]
        json_groups = [json.dumps(group) for group in groups if len(group) > 0]
        sqs_messages = [(i, json, 0) for i, json in enumerate(json_groups)]
        sqs_queue.write_batch(sqs_messages)

    kwargs = {"flush_fn": flush_fn}
    if config.has_section("Flush"):
        if config.has_option("Flush", "Seconds"):
            kwargs["flush_seconds"] = config.getfloat("Flush", "Seconds")
        if config.has_option("Flush", "Lines"):
            flush_lines = config.getint("Flush", "Lines")
            if flush_single and flush_lines > 10:
                raise Error("Cannot send more than 10 messages to SQS in a flush")
            kwargs["flush_lines"] = flush_lines

    _queue = TimeAndSizeFlushingQueue(**kwargs)

def deinit():
    global _queue
    _queue.close()
    _queue = None

if __name__ == "__main__":
    import doctest
    doctest.testmod(optionflags=doctest.ELLIPSIS)
