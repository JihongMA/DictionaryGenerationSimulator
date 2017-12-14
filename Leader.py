from Base_class import *
import gc
import multiprocessing as mp
import threading as th
import random
import sys
from timeit import default_timer as timer


class Leader_tester(Tester):
    # Pipe from leader to each worker
    def make_pipes(self, worker_num):
        leader_pipes = []
        worker_pipes = []
        for i in range(worker_num):
            leader_side, worker_side = mp.Pipe()
            leader_pipes.append(leader_side)
            worker_pipes.append(worker_side)
        return worker_pipes, leader_pipes

    def run_Leader_test(self, worker_num, files, buffer_size):
        add_requests = mp.Queue()
        worker_pipes, leader_pipes = self.make_pipes(worker_num)
        children = []
        leader_num_sent = mp.Value('I', 0)
        leader_key_conflict = mp.Value('I', 0)
        worker_num_sent_list = []
        start = timer()
        dic = self.look_ahead()
        alook = timer()
        tlook = alook-start
        #dic = dict()
        leader = mp.Process(target=Leader_DL, args=(worker_num, leader_pipes, add_requests, leader_num_sent, leader_key_conflict, self.delay, dic))
        for w in range(worker_num):
            worker_num_sent = mp.Value('I', 0)
            p = mp.Process(target=Worker_DL, args=(w, worker_pipes[w], add_requests, files[w], self.fieldnum, buffer_size, worker_num_sent, self.delay, dic))
            worker_num_sent_list.append(worker_num_sent)
            children.append(p)

        gc.collect()

        leader.start()
        for p in children:
            p.start()
        leader.join()
        for p in children:
            p.join()
        end = timer()
        elapsed = end - start
        tdict = end-alook
        num_messages_sent = leader_num_sent.value + sum(map(lambda x: x.value, worker_num_sent_list))
        num_key_conflict = leader_key_conflict.value
        return elapsed, tlook, tdict, num_messages_sent, num_key_conflict, 0

    def run_test(self):
        return self.run_Leader_test(self.worker_num, self.files, self.buffer_size)
# All new keys in each proposal could be partially accepted or rejected
class Leader_DL(Peer):
    def __init__(self, worker_num, pipes, add_requests, ret, confl, delay, dic):
        self.worker_num = worker_num # total number of workers
        self.pipes = pipes
        self.ar = add_requests
        self.delay = delay
        self.d = dict(dic)
        self.sent = 0
        self.key_conflict = 0
        self.next_val = len(dic)
        ret.value, confl.value = self.run()

    def run(self):
        workers_done = 0
        while workers_done < self.worker_num:
            proposed_keys = self.ar.get()
            if proposed_keys == None:
                workers_done += 1
                continue
            new_keys = {}
            for k in proposed_keys:
                if k not in self.d:
                    self.d[k] = self.next_val
                    new_keys[k] = self.next_val
                    self.next_val += 1
                else:
                    self.key_conflict += 1
            if len(new_keys) > 0:
                for p in self.pipes:
                    self.my_send(p, new_keys)
        for p in self.pipes:
            self.my_send(p, None)
            p.close()

        # print("Leader done (sent {} messages)".format(self.sent))
        return self.sent, self.key_conflict

class Worker_DL(Peer):
    def __init__(self, worker_num, leader_pipe, add_requests, file, fieldnum, buffer_size, ret, delay, dic):
        self.worker_num = worker_num
        self.lp = leader_pipe
        self.ar = add_requests
        self.file = file
        self.fieldnum = fieldnum
        self.buffer_size = buffer_size
        self.delay = delay
        self.d = dict(dic)
        self.sent = 0
        self.next_val = len(dic)
        ret.value = self.run()

    def listen_for_new_keys(self):
        while (True):
            new_keys = self.lp.recv()
            # print("{} received {}".format(self.worker_num, to_add))

            if new_keys == None:
                break

            for k, v in new_keys.items():
                assert k not in self.d
                self.d[k] = v


    def run(self):
        # d_lock = th.Lock()
        t = th.Thread(target=self.listen_for_new_keys, args=())
        t.start()

        new_keys_list = []
        with open(self.file) as f:
            for line in f:
                line = line.strip()

                key = line.split(",")[self.fieldnum]
                if key not in self.d:
                    new_keys_list.append(key)
                    if len(new_keys_list) == self.buffer_size:
                        self.sent += 1
                        self.ar.put(new_keys_list)
                        new_keys_list = []
        if len(new_keys_list) > 0:
            self.sent += 1
            self.ar.put(new_keys_list)
            new_keys_list = 0
        # Done parsing
        self.ar.put(None)

        t.join()
        self.lp.close()
        # print("{} has dict: {}".format(worker_num, d))
        # print("{} done (sent {} messages)".format(worker_num, sent))
        return self.sent
