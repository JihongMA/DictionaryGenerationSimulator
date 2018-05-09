# coding=utf-8
import numpy as np
from time import sleep
import threading as th
import sched, time

# Base class for each component
class Peer():
    def __init__(self, worker_num, pipes, ret, delay):
        self.worker_num = worker_num
        self.pipes = pipes
        self.sent = 0
        self.delay = delay
        self.sch = sched.scheduler(time.time, time.sleep)
        ret.value = self.run()
        self.d = dict()

    def set_dictionary(self, dictionary):
        self.d = dict(dictionary)

    def my_send(self, p, to_send):
        self.my_sched_send(p, to_send)
    # Add latency to each message
    def my_delay_send(self, p, to_send):
        if self.delay>0:
            mu, sigma = 0, 0.1 # mean and standard deviation
            normrand = np.random.normal(mu, sigma, 1)
            t = normrand/1000.0 + self.delay
            t_sleep = t if t>=0 else 0.0
            sleep(t_sleep)

        self.sent += 1
        # print("{} sending {}".format(self.worker_num, to_send))
        p.send(to_send)

    def my_sched_send(self, p, to_send):
        t_sleep = 0
        if self.delay>0:
            mu, sigma = 0, 0.1 # mean and standard deviation
            normrand = np.random.normal(mu, sigma, 1)
            t = normrand/1000.0 + self.delay
            t_sleep = t if t>=0 else 0.0
        self.sch.enter(t_sleep, 1, p.send, (to_send,))
        self.sent += 1


    def run(self):
        return self.sent


# Base class for tester
class Tester():
    def __init__(self, worker_num, files, buffer_size, delay, look, tuple_num, fieldnum, order, scale):
        self.worker_num = worker_num
        self.files = files
        self.buffer_size = buffer_size
        self.delay = delay
        self.look = look
        self.tuple_per_file = tuple_num
        self.fieldnum = fieldnum
        self.count = 0
        self.order = order
        self.scale = scale

    # Look ahead and pre-build part of dictionary. Call scanning from the beginning model when look in (0.0, 1.0) and uniform sampling when look (percentage) in [1, 100]
    # adaptive look ahead（continue look ahead until no more new key found）and reservoir sampling
    def look_ahead(self):
        dic = dict()
        if self.look <= 1.0:
            num_readahead = int(self.look * self.tuple_per_file)
            if (self.order):
                keys = set()
                for fil in self.files:
                    with open(fil) as f:
                        scount = 0
                        for line in f:
                            if scount >= num_readahead:
                                break
                            line = line.strip()
                            key = line.split(",")[self.fieldnum]
                            scount += 1
                            keys.add(key)
                sortKey = sorted(keys)
                for sk in sortKey:
                    dic[sk] = self.count
                    self.count += 1
            else:
                for fil in self.files:
                    with open(fil) as f:
                        scount = 0
                        for line in f:
                            if scount >= num_readahead:
                                break
                            line = line.strip()
                            key = line.split(",")[self.fieldnum]
                            scount += 1
                            if key not in dic:
                                dic[key] = self.count
                                self.count += 1
        elif self.look <= 100:
            if (self.order):
                keys = set()
                for fil in self.files:
                    with open(fil) as f:
                        scount = 0
                        for line in f:
                            scount += 1
                            if scount%100 <= self.look:
                                line = line.strip()
                                key = line.split(",")[self.fieldnum]
                                keys.add(key)
                sortKey = sorted(keys)
                for sk in sortKey:
                    dic[sk] = self.count
                    self.count += 1
            else:
                for fil in self.files:
                    with open(fil) as f:
                        scount = 0
                        for line in f:
                            scount += 1
                            if scount%100 <= self.look:
                                line = line.strip()
                                key = line.split(",")[self.fieldnum]
                                if key not in dic:
                                    dic[key] = self.count
                                    self.count += 1
                            else:
                                pass
        #print dic
        return dic


    def run_test(self):
        raise Exception("Abstract Class")

    def make_pipes(self):
        raise Exception("Abstract Class")
