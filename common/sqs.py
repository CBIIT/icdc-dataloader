#!/usr/bin/env python
import boto3
import json
from threading import Timer

from .utils import get_logger
from .config import QUEUE_LONG_PULL_TIME

class Queue:
    def __init__(self, queue_name):
        self.log = get_logger('SQS')
        self.sqs = boto3.resource('sqs')
        self.queue = self.sqs.get_queue_by_name(QueueName=queue_name)

    def sendMsgToQueue(self, msg, msg_id):
        response = self.queue.send_message(MessageBody=json.dumps(msg),
                                           MessageGroupId=msg_id,
                                           MessageDeduplicationId=msg_id)
        self.log.debug(response.get('MessageId'))

    def receiveMsgs(self, visibilityTimeOut):
        return self.queue.receive_messages(VisibilityTimeout = visibilityTimeOut,
                                           WaitTimeSeconds = QUEUE_LONG_PULL_TIME,
                                           MaxNumberOfMessages = 1)

    def getApproximateNumberOfMessages(self):
        return self.queue.attributes.get('ApproximateNumberOfMessages', -1)

# Automatically extend visibility timeout every timeOutValue // 2 seconds
class VisibilityExtender:
    def __init__(self, msg, timeOutValue):
        self._timeOutValue = timeOutValue if timeOutValue > 2 else 2
        self._currentTimeOut = self._timeOutValue
        self._interval = int(timeOutValue // 2) if timeOutValue > 2 else 1
        self._msg = msg
        self._timer = None
        self.is_running = False
        self.log = get_logger('Visibility Extender')
        self.start()

    def _run(self):
        try:
            if self._msg:
                self.is_running = False
                self.start()
                self._currentTimeOut += self._interval
                self.log.info('Processing job ..., visibility timeout = {}s'.format(self._currentTimeOut))
                self._msg.change_visibility(VisibilityTimeout = self._currentTimeOut)
        except Exception as e:
            self.log.exception(e)


    def start(self):
        if not self.is_running:
            self._timer = Timer(self._interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False

    def __del__(self):
        self.stop()
