from Base_class import *
import gc
import multiprocessing as mp
import threading as th
import random
import sys
import math
import collections
from heapq import merge
from timeit import default_timer as timer


class MostlyOP_tester(Tester):
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
        lookahead_dic = self.look_ahead()
        print('lookahead dictionary size',len(lookahead_dic))
        dic = dict()
        if (self.look>0):
            scale = int(math.ceil(self.scale/self.look))
            for k, v in  lookahead_dic.items():
                dic[k] = (v+1)*scale
        else:
            scale = 0
        alook = timer()
        tlook = alook-start

        leader = mp.Process(target=Leader_MOP, args=(worker_num, leader_pipes, add_requests, leader_num_sent, leader_key_conflict, self.delay, dic, scale*(len(dic)+1)))
        for w in range(worker_num):
            worker_num_sent = mp.Value('I', 0)
            p = mp.Process(target=Worker_MOP, args=(w, worker_pipes[w], add_requests, files[w], self.fieldnum, buffer_size, worker_num_sent, self.delay, dic))
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
class Leader_MOP(Peer):
    def __init__(self, worker_num, pipes, add_requests, ret, confl, delay, dic, backupstart):
        self.worker_num = worker_num # total number of workers
        self.pipes = pipes
        self.ar = add_requests
        self.delay = delay
        self.d = dict(dic)
        self.sch = sched.scheduler(time.time, time.sleep)
        self.sent = 0
        self.key_conflict = 0
        self.backup = backupstart
        self.orderdkey = sorted(dic.keys())
        self.next_val = backupstart
        ret.value, confl.value = self.run()

    def run(self):
        workers_done = 0
        packet_count = 0
        new_key_combine=set()
        dict_set = set(self.d.keys())
        dict_list = sorted(dict_set)
        while workers_done < self.worker_num:
            proposed_keys = self.ar.get()
            if proposed_keys == None:
                workers_done += 1
                continue
            packet_count += 1
            new_key_combine = new_key_combine | proposed_keys
            #print('proposed_keys', len(proposed_keys), proposed_keys)
            new_keys = {}
            if packet_count == self.worker_num:
                start = 0
                end = self.backup
                cur = False
                pre = True
                #print('new key combine before', len(new_key_combine), new_key_combine)
                new_key_combine = new_key_combine - dict_set
                #print('new key combine after', len(new_key_combine), new_key_combine)
                dict_set = dict_set | new_key_combine
                sorted_new_key = sorted(new_key_combine)
                dict_list = list(merge(dict_list,sorted_new_key))
                sort_dict_key = dict_list
                for i in range(len(sort_dict_key)):
                    if sort_dict_key[i] in self.d:
                        cur = True
                    else:
                        cur = False
                    if cur == False and pre == True:
                        start = i-1
                    elif cur == True and pre == False:
                        end = i
                        if start<0:
                            spaceStart = -1
                            spaceEnd = self.d[sort_dict_key[end]]
                            if (spaceEnd-spaceStart)>(end-start):
                                delta = int(math.floor((spaceEnd-spaceStart)/(end-start)))
                                for key in range(start+1,end, 1):
                                    spaceStart = spaceStart+delta
                                    self.d[sort_dict_key[key]] = spaceStart
                                    new_keys[sort_dict_key[key]] = spaceStart

                            else:
                                delta = 1
                                for key in range(start+1,end, 1):
                                    spaceStart = spaceStart+delta
                                    if spaceStart>=spaceEnd:
                                        self.d[sort_dict_key[key]] = self.next_val
                                        new_keys[sort_dict_key[key]] = self.next_val
                                        self.next_val+=1
                                    else:
                                        self.d[sort_dict_key[key]] = spaceStart
                                        new_keys[sort_dict_key[key]] = spaceStart

                        elif end<self.backup:
                            spaceStart = self.d[sort_dict_key[start]]
                            spaceEnd = self.d[sort_dict_key[end]]
                            if (spaceEnd-spaceStart)>(end-start):
                                delta = int(math.floor((spaceEnd-spaceStart)/(end-start)))
                                for key in range(start+1,end, 1):
                                    spaceStart = spaceStart+delta
                                    self.d[sort_dict_key[key]] = spaceStart
                                    new_keys[sort_dict_key[key]] = spaceStart
                            else:
                                delta = 1
                                for key in range(start+1,end, 1):
                                    spaceStart = spaceStart+delta
                                    if spaceStart>=spaceEnd:
                                        self.d[sort_dict_key[key]] = self.next_val
                                        new_keys[sort_dict_key[key]] = self.next_val
                                        self.next_val+=1
                                    else:
                                        self.d[sort_dict_key[key]] = spaceStart
                                        new_keys[sort_dict_key[key]] = spaceStart
                    pre = cur
                if pre == False:
                    spaceStart = self.d[sort_dict_key[start]]
                    spaceEnd = self.backup
                    end = len(sort_dict_key)
                    if (spaceEnd-spaceStart)>(end-start):
                        delta = int(math.floor((spaceEnd-spaceStart)/(end-start)))
                        for key in range(start+1,end, 1):
                            spaceStart = spaceStart+delta
                            self.d[sort_dict_key[key]] = spaceStart
                            new_keys[sort_dict_key[key]] = spaceStart
                    else:
                        delta = 1
                        for key in range(start+1,end, 1):
                            spaceStart = spaceStart+delta
                            if spaceStart>=spaceEnd:
                                self.d[sort_dict_key[key]] = self.next_val
                                new_keys[sort_dict_key[key]] = self.next_val
                                self.next_val+=1
                            else:
                                self.d[sort_dict_key[key]] = spaceStart
                                new_keys[sort_dict_key[key]] = spaceStart
                if len(new_keys) > 0:
                    #print('new key sent', len(new_keys), new_keys)
                    for p in self.pipes:
                        self.my_send(p, new_keys)
                    self.sch.run()
                packet_count = 0
                new_key_combine = set()
                new_keys = {}

        if len(new_key_combine)>0:
            start = 0
            end = self.backup
            cur = False
            pre = True
            new_key_combine = new_key_combine-dict_set
            dict_set = dict_set | new_key_combine
            sort_dict_key = sorted(dict_set)
            sorted_new_key = sorted(new_key_combine)
            for i in range(len(sort_dict_key)):
                if sort_dict_key[i] in self.d:
                    cur = True
                else:
                    cur = False
                if cur == False and pre == True:
                    start = i-1
                elif cur == True and pre == False:
                    end = i
                    if start<0:
                        spaceStart = -1
                        spaceEnd = self.d[sort_dict_key[end]]
                        if (spaceEnd-spaceStart)>(end-start):
                            delta = int(math.floor((spaceEnd-spaceStart)/(end-start)))
                            for key in range(start+1,end, 1):
                                spaceStart = spaceStart+delta
                                self.d[sort_dict_key[key]] = spaceStart
                                new_keys[sort_dict_key[key]] = spaceStart

                        else:
                            delta = 1
                            for key in range(start+1,end, 1):
                                spaceStart = spaceStart+delta
                                if spaceStart>=spaceEnd:
                                    self.d[sort_dict_key[key]] = self.next_val
                                    new_keys[sort_dict_key[key]] = self.next_val
                                    self.next_val+=1
                                else:
                                    self.d[sort_dict_key[key]] = spaceStart
                                    new_keys[sort_dict_key[key]] = spaceStart

                    elif end<self.backup:
                        spaceStart = self.d[sort_dict_key[start]]
                        spaceEnd = self.d[sort_dict_key[end]]
                        if (spaceEnd-spaceStart)>(end-start):
                            delta = int(math.floor((spaceEnd-spaceStart)/(end-start)))
                            for key in range(start+1,end, 1):
                                spaceStart = spaceStart+delta
                                self.d[sort_dict_key[key]] = spaceStart
                                new_keys[sort_dict_key[key]] = spaceStart
                        else:
                            delta = 1
                            for key in range(start+1,end, 1):
                                spaceStart = spaceStart+delta
                                if spaceStart>=spaceEnd:
                                    self.d[sort_dict_key[key]] = self.next_val
                                    new_keys[sort_dict_key[key]] = self.next_val
                                    self.next_val+=1
                                else:
                                    self.d[sort_dict_key[key]] = spaceStart
                                    new_keys[sort_dict_key[key]] = spaceStart
                pre = cur
            if pre == False:
                spaceStart = self.d[sort_dict_key[start]]
                spaceEnd = self.backup
                end = len(sort_dict_key)
                if (spaceEnd-spaceStart)>(end-start):
                    delta = int(math.floor((spaceEnd-spaceStart)/(end-start)))
                    for key in range(start+1,end, 1):
                        spaceStart = spaceStart+delta
                        self.d[sort_dict_key[key]] = spaceStart
                        new_keys[sort_dict_key[key]] = spaceStart
                else:
                    delta = 1
                    for key in range(start+1,end, 1):
                        spaceStart = spaceStart+delta
                        if spaceStart>=spaceEnd:
                            self.d[sort_dict_key[key]] = self.next_val
                            new_keys[sort_dict_key[key]] = self.next_val
                            self.next_val+=1
                        else:
                            self.d[sort_dict_key[key]] = spaceStart
                            new_keys[sort_dict_key[key]] = spaceStart
            if len(new_keys) > 0:
                #print(len(new_keys))
                for p in self.pipes:
                    self.my_send(p, new_keys)
                self.sch.run()
            packet_count = 0
            new_key_combine=set()
            new_keys = {}
        for p in self.pipes:
            self.my_send(p, None)
            self.sch.run()
            p.close()
        #od = collections.OrderedDict(sorted(self.d.items()))
        #print(od)
        print("OP,{},{},{},{}".format(len(self.d), self.backup, self.next_val, (self.next_val-self.backup)*1.0/len(self.d)))
        return self.sent, self.key_conflict

class Worker_MOP(Peer):
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

        new_keys_set = set()
        new_keys_list = []
        with open(self.file) as f:
            for line in f:
                line = line.strip()

                key = line.split(",")[self.fieldnum]
                if key not in self.d:
                    new_keys_list.append(key)
                    new_keys_set.add(key)
                    if len(new_keys_list) == self.buffer_size:
                        self.sent += 1
                        #print('send',len(new_keys_set))
                        self.ar.put(new_keys_set)
                        #print('new key combine before', len(new_keys_set), new_keys_set)
                        new_keys_list = []
                        new_keys_set = set()
        if len(new_keys_list) > 0:
            self.sent += 1
            #print('send',len(new_keys_set))
            self.ar.put(new_keys_set)
            new_keys_list = 0
        # Done parsing
        self.ar.put(None)

        t.join()
        self.lp.close()
        # print("{} has dict: {}".format(worker_num, d))
        # print("{} done (sent {} messages)".format(worker_num, sent))
        return self.sent
