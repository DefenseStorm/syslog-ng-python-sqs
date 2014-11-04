# README

This is a Python destination driver for syslog-ng which writes messages to SQS in batches.  Messages are flushed to SQS based on time and count, and batched to minimize cost and maximize throughput.  Each SQS Message contains 1 or more Syslog messages, serialized as a JSON array of objects\*.

\* If the Flush.Single config flag is set, SQS Messages each contain exactly one Syslog message serialized as a JSON object

## TODOs

- [x] Add a flag to disable grouping of messages to SQS
- [ ] Figure out what to do if a serialized group or batch exceeds SQS size limits
- [ ] Handle exceptions from calling SQS and retry

## Prerequisites

* syslog-ng
* syslog-ng-incubator (syslog-ng-mod-python)
* boto (python-boto)

## Run Tests

`python python_sqs.py`

## Example Test Driver

```
import python_sqs

python_sqs.init()
python_sqs.queue({'message': 'text', 'src_ip': '192.168.0.1'})
python_sqs.deinit()
```

## Example Config File

```
[AWS]
Region=us-west-2
AccessKeyID=COOLESTKEYEVER
SecretAccessKey=MOSTSECRETKEYONEARTH
SQSQueueName=syslog-queue

[Flush]
Single=False
Seconds=1
Lines=100
```
