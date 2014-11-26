#!/usr/bin/env python
import json, logging, sys
import python_sqs

log = None
line = None
try:
    python_sqs.init(json_input=True)
    log = logging.getLogger(__name__)
    line = sys.stdin.readline()
    while line:
        python_sqs.queue(line.strip())
        line = sys.stdin.readline()
except e:
    if log:
        log.error("Error while processing %s", line, exc_info=e)
    else:
        print e
finally:
    try:
        python_sqs.deinit()
    except e:
        print e
