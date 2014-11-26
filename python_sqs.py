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

log = None

class TimeAndSizeFlushingQueue:

    def __init__(self, flush_fn=None, flush_seconds=None, flush_lines=None, flush_bytes=None):
        self._timer = None
        # Deadlock semantics: NEVER take both locks on the same thread
        self._timer_lock = Lock() # Guards all changes to `timer`
        self._queue = deque()
        self._flush_lock = Lock() # Guards consumption of `self._queue`

        self._fn = flush_fn
        self._seconds = flush_seconds
        self._lines = flush_lines
        self._bytes = flush_bytes
        if self._bytes is not None:
            self._queued_bytes = 0
            self._bytes_lock = Lock()

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
                        or self._seconds is not None and time.time() - self._queue[0][0] >= self._seconds
                        or self._full_of_bytes()))
        except IndexError:
            return False

    def _full_of_bytes(self):
        if self._bytes is None:
            return False
        with self._bytes_lock:
            return self._queued_bytes >= self._bytes

    def _add_queued_bytes(self, delta):
        if self._bytes is None:
            return
        with self._bytes_lock:
            self._queued_bytes += delta

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
                flushed_bytes = 0
                messages = []
                while len(messages) < self._lines:
                    try:
                        message = self._queue.popleft()[1]
                        if self._bytes is not None:
                            if len(message) > self._bytes:
                                log.error("Message too large to flush (%d > %d): %s",
                                        len(message), self._bytes, message)
                                continue
                            elif flushed_bytes + len(message) > self._bytes:
                                self._queue.appendleft((0, message))
                                break
                        flushed_bytes += len(message)
                        messages.append(message)
                    except IndexError:
                        break
                self._add_queued_bytes(-flushed_bytes)
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
        >>> queue = TimeAndSizeFlushingQueue(flush_seconds=1, flush_lines=2)
        >>> queue.queue("a message")
        >>> time.sleep(.001)
        >>> queue._timer
        <_Timer(Thread-..., started ...)>
        >>> queue.queue("a message")
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
        self._add_queued_bytes(len(message))
        if self._should_flush():
            self._flush_queue()
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
    log.debug("Queueing message %s", message)
    global _queue
    try:
        _queue.queue(message)
    except Exception, e:
        log.error("Error queueing message %s", message, exc_info=e)

def init(json_input=False):
    global _queue, log
    from ConfigParser import SafeConfigParser
    import sys, logging, logging.config
    import boto.sqs as sqs
    config = SafeConfigParser()
    config.read('/etc/syslog-ng/python_sqs.conf')

    if config.has_option("Logging", "Level"):
        log_level = config.get("Logging", "Level")
    else:
        log_level = 'DEBUG'
    log_config = {
            'version': 1,
            'formatters': {
                'syslog': {
                    'format': '%(name)s[%(process)d]: %(threadName)s %(filename)s#%(lineno)d %(message)s'
                }
            },
            'handlers': {
                'syslog': {
                    'class': 'logging.handlers.SysLogHandler',
                    'level': log_level,
                    'formatter': 'syslog',
                    'address': '/dev/log',
                    'facility': 'syslog'
                }
            },
            'root': {
                'level': log_level,
                'handlers': ['syslog']
            }
        }
    if config.has_option("Logging", "Filename"):
        log_config['handlers']['file'] = {
                'class': 'logging.handlers.RotatingFileHandler',
                'maxBytes': 10*1024*1024, # 10MiB
                'level': log_level,
                'filename': config.get("Logging", "Filename")
            }
        log_config['root']['handlers'].append('file')
    logging.config.dictConfig(log_config)
    log = logging.getLogger(__name__)
    log.debug("Python SQS initialized...")

    conn = sqs.connect_to_region(
        config.get("AWS", "Region"),
        aws_access_key_id=config.get("AWS", "AccessKeyID"),
        aws_secret_access_key=config.get("AWS", "SecretAccessKey"))
    sqs_queue = conn.get_queue(config.get("AWS", "SQSQueueName"))

    flush_single = config.has_option("Flush", "Single") and config.getboolean("Flush", "Single")
    flush_retries = config.has_option("Flush", "Retries") and config.getint("Flush", "Retries")
    def flush_fn(messages, retries=flush_retries):
        log.debug("Flushing %d messages", len(messages))
        try:
            if flush_single:
                groups = messages
            else:
                groups = [messages[i::10] for i in range(10)]
            log.debug("Messages grouped as %s", [len(g) for g in groups])
            if json_input and flush_single:
                json_groups = groups
            elif json_input:
                json_groups = ['[' + ','.join(message for message in group) + ']' for group in groups if group]
            else:
                import json
                json_groups = [json.dumps(group) for group in groups if group]
            log.debug("Messages converted to json %s", [len(g) for g in json_groups])
            sqs_messages = [(i, json, 0) for i, json in enumerate(json_groups)]
            log.debug("Messages prepped for SQS")
            br = sqs_queue.write_batch(sqs_messages)
            if br.results:
                log.debug("Successfully flushed %d groups: %s", len(br.results), br.results)
            if br.errors:
                messages = []
                for error in br.errors:
                    messages += groups[error['id']]
                raise Exception("Failed to flush %d groups: %s" % (len(br.errors), br.errors))
        except Exception, e:
            if retries:
                log.warn("Error flushing %d messages, trying smaller batch (%d tries left)",
                        len(messages), retries, exc_info=e)
                flush_fn(messages[::2], retries - 1)
                flush_fn(messages[1::2], retries - 1)
            else:
                log.error("Failed to flush %d messages", len(messages), exc_info=e)
                for message in messages:
                    log.error("Failed: %s", message)

    kwargs = {"flush_fn": flush_fn}
    if config.has_option("Flush", "Bytes"):
        kwargs["flush_bytes"] = config.getfloat("Flush", "Bytes")
    else:
        kwargs["flush_bytes"] = 250000 # Safely below SQS limit
    if config.has_option("Flush", "Seconds"):
        kwargs["flush_seconds"] = config.getfloat("Flush", "Seconds")
    if config.has_option("Flush", "Lines"):
        flush_lines = config.getint("Flush", "Lines")
        if flush_single and flush_lines > 10:
            raise Error("Cannot send more than 10 messages to SQS in a flush")
        kwargs["flush_lines"] = flush_lines

    _queue = TimeAndSizeFlushingQueue(**kwargs)

def deinit():
    log.debug("Deinitializing Python SQS")
    global _queue
    _queue.close()
    _queue = None

if __name__ == "__main__":
    import doctest
    doctest.testmod(optionflags=doctest.ELLIPSIS)
