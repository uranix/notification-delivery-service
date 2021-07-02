import logging
import time
import threading
import random

from queue import PriorityQueue, Full

logger = logging.getLogger(__name__)


class Message:
    def __init__(self, body, send_at, queued_at, attempt=0):
        self.body = body
        self.send_at = send_at
        self.queued_at = queued_at
        self.attempt = attempt

    def make_next_attempt(self, delay):
        send_at = time.time() + delay  # schedule send at now + delay sec
        return Message(self.body, send_at, self.queued_at, self.attempt + 1)

    def __lt__(self, other):
        return self.send_at < other.send_at

    def __repr__(self):
        body = self.body
        body = body[:17] + '...' if len(body) > 20 else body
        delta = self.send_at - time.time()
        return 'Message{%s, attempt=%d, send_at=now%+gs}' % (body, self.attempt, delta)


class SendQueue(PriorityQueue):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def accept(self, now, body):
        message = Message(body, now, now)
        try:
            super().put_nowait(message)
        except Full:
            return False
        return True


# https://en.wikipedia.org/wiki/Token_bucket
class RateLimiter:
    def __init__(self, bucket_size, window_secs):
        self.bucket_size = bucket_size
        self.last_tick = time.time()
        self.available = bucket_size
        self.window_secs = window_secs

    def is_allowed(self):
        now = time.time()
        passed = now - self.last_tick
        self.last_tick = now
        self.available += passed * self.bucket_size / self.window_secs
        if self.available > self.bucket_size:
            self.available = self.bucket_size
        if self.available < 1:
            return False
        self.available -= 1
        return True


class SenderThread(threading.Thread):
    def __init__(self, queue):
        super().__init__(name="Sender")
        self.setDaemon(True)
        self.queue = queue
        self.rate_limiter = RateLimiter(10, 5)  # 10 msg per 5s

    def run(self):
        while True:
            while True:
                item = self.queue.get()
                now = time.time()
                logger.debug('Dequeued %s at %s', item, now)
                if item.send_at > now:
                    logger.debug('Too early, enqueued %s back', item)
                    self.queue.put(item)
                    break
                self.try_send(item)

            time.sleep(0.1)

    def try_send(self, item):
        logger.debug('Trying to send %s', item)
        try:
            if self.send(item):
                logger.info('Sent %s', item)
            else:
                # Send failed, schedule retry
                delay = 0.75 + 0.5 * random.random()
                item = item.make_next_attempt(delay)
                self.queue.put(item)
                logging.info('Postponed message %s', item)
        except Exception as e:
            logger.error('Exception in try_send', e)

    def send(self, item):
        # /dev/null is a proper place for spam
        if not self.rate_limiter.is_allowed():
            return False
        lag = time.time() - item.queued_at
        logger.debug('Sending %s with lag %gs', item, lag)
        send_time = 0.2 + 0.3 * random.random()
        time.sleep(send_time)
        return True
