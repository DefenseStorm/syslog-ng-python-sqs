#!/usr/bin/env python
import json, logging, sys
import python_sqs

log = None
line = None
encodings = ('UTF-8', 'WINDOWS-1252', 'ISO-8859-1')
try:
    python_sqs.init(json_input=True)
    log = logging.getLogger(__name__)
    line = sys.stdin.readline()
    while line:
        line = line.strip()
        for encoding in encodings:
            try:
                line = unicode(line, encoding)
                break
            except ValueError:
                pass
        else:
            log.debug("Falling back to ascii w/replace")
            line = unicode(line, errors='replace')
        python_sqs.queue(line)
        line = sys.stdin.readline()
except Exception, e:
    if log:
        log.error("Error while processing %s", line, exc_info=e)
    else:
        print e
finally:
    try:
        python_sqs.deinit()
    except Exception, e:
        print e
