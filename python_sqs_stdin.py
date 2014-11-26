#!/usr/bin/env python
from __future__ import with_statement
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
except Exception, e:
    if log:
        log.error("Error while processing %s", line, exc_info=e)
finally:
    python_sqs.deinit()
