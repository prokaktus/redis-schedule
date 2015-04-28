#!/usr/bin/env python3
import redis
import time

class RedisSchedule(redis.StrictRedis):
    def __init__(self, name="default"):
        super().__init__()
        self.name_set = name
        self.id = 'id:' + self.name_set
        self.set(self.id, 0, nx=True)
        
    def add_timestamp(self, timestamp, data=None):
        data = data or self.incr(self.id)
        self.zadd(self.name_set, timestamp, data)

    def add_timestamps(self, list_elem):
        for i in list_elem:
            self.add_timestamp(i)

    def poll(self, score=None):
        if not score:
            score = int(time.time())
        result = self.zrangebyscore(self.name_set, 0, score)
        self.zremrangebyscore(self.name_set, 0, score)
        return result
        
    
