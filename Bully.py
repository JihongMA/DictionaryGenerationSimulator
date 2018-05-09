from Base_class import *
import multiprocessing as mp
import random
import sys
import time
import gc
from timeit import default_timer as timer
NUMPERALLOC = 200
MAXWAITING = 0.05


class Bully_2PC_tester(Tester):

    # 2D array of pipes (worker_num * (worker_num - 1))
    # 1D array of pipes for value space alloc
    def make_pipes(self, worker_num):
        manager_pipes = []
        worker_pipes = []
        for i in range(worker_num):
            manager_side, worker_side = mp.Pipe()
            manager_pipes.append(manager_side)
            worker_pipes.append(worker_side)

        pipes = []
        for w in range(worker_num):
            pipes.append(worker_num * [None])
        for i in range(worker_num):
            for j in range(i+1, worker_num):
                pipes[i][j], pipes[j][i] = mp.Pipe()
            del pipes[i][i]
        return pipes, manager_pipes, worker_pipes

    def run_2PC_test(self, worker_num, files, buffer_size):
        pipes, manager_pipes, worker_pipes = self.make_pipes(worker_num)
        children = []
        worker_num_sent_list = []
        leader_key_conflict_list = []
        worker_num_abort_list = []
        manager_num_sent = mp.Value('I', 0)
        start = timer()
        dic = self.look_ahead()
        alook = timer()
        tlook = alook-start
        #dic = dict()
        #print dic
        manager = mp.Process(target=Value_space_manager, args=(worker_num, manager_pipes, manager_num_sent, self.delay, dic))
        worker_num_sent_list.append(manager_num_sent)
        # Start each work node
        for w in range(worker_num):
            worker_num_sent = mp.Value('I', 0)
            leader_key_conflict = mp.Value('I', 0)
            worker_num_abort = mp.Value('I', 0)
            p = mp.Process(target=Worker_2PC_bully, args=(w, pipes[w], worker_pipes[w], files[w], self.fieldnum, buffer_size, worker_num_sent, leader_key_conflict, worker_num_abort, self.delay, dic))
            worker_num_sent_list.append(worker_num_sent)
            leader_key_conflict_list.append(leader_key_conflict)
            worker_num_abort_list.append(worker_num_abort)
            children.append(p)

        gc.collect()


        manager.start()
        for p in children:
            p.start()
        for p in children:
            p.join()
        end = timer()
        elapsed = end - start
        tdict = end - alook
        num_messages_sent = sum(map(lambda x: x.value, worker_num_sent_list))
        num_key_conflict = sum(map(lambda x: x.value, leader_key_conflict_list))
        num_abort = sum(map(lambda x: x.value, worker_num_abort_list))
        return elapsed, tlook, tdict, num_messages_sent, num_key_conflict, num_abort

    def run_test(self):
        return self.run_2PC_test(self.worker_num, self.files, self.buffer_size)

class Value_space_manager(Peer):
    def __init__(self, worker_num, pipes, ret, delay, dic):
        # Total number of workers
        self.worker_num = worker_num
        self.value_for_alloc = len(dic)
        self.pipes = pipes
        self.sent = 0
        self.sch = sched.scheduler(time.time, time.sleep)
        self.delay = delay
        ret.value = self.run()

    def make_VALUE_SPACE_ALLOC(self, recipient, received):
        to_send = {
            "worker_num": "VALUE_SPACE_MANAGER",
            "recipient": received["worker_num"],
            "type": "VALUE_SPACE_ALLOC",
            "new_start_value": self.value_for_alloc,
        }
        return to_send

    def run(self):
        worker_done = 0
        while worker_done<self.worker_num:
            for p in self.pipes:
                while p.poll():
                    received = p.recv()
                    if received["type"] == "2PC_DONE":
                        worker_done += 1
                    else:
                        assert received["type"] == "APPLY_VALUE_SPACE"
                        sender = received["worker_num"]
                        self.my_send(p, self.make_VALUE_SPACE_ALLOC(sender,received))
                        self.sch.run()
                        self.value_for_alloc += NUMPERALLOC
                    #print("manager {} sends {}").format(self.worker_num, self.value_for_alloc)
        for p in self.pipes:
            p.close()
        #print("Value Manager exit successfully!")
        return self.sent


# All new keys in each proposal will be accepted or rejected as a whole
class Worker_2PC_bully(Peer):
    def __init__(self, worker_num, pipes, pipe, file, fieldnum, buffer_size, ret, confl, abort, delay, dic):
        self.worker_num = worker_num
        self.pipes = pipes
        self.manager_pipe = pipe
        self.file = file
        self.fieldnum = fieldnum
        self.alloc = 0
        # Number of keys to buffer before blocking to propose them
        self.buffer_size = buffer_size
        self.sch = sched.scheduler(time.time, time.sleep)
        self.d = dict(dic)
        self.next_value = 1
        self.done = []
        self.delay = delay
        self.sent = 0
        self.key_conflict = 0
        self.abort = 0
        self.saved_proposal = []
        for w in range(len(self.pipes)+1):
            self.saved_proposal.append(2 * [None])

        ret.value, confl.value, abort.value = self.run()

    def make_APPLY_VALUE_SPACE(self):
        to_send = {
            "worker_num": self.worker_num,
            "recipient": "VALUE_SPACE_MANAGER",
            "type": "APPLY_VALUE_SPACE",
        }
        return to_send

    def make_2PC_NEW_KEY(self, recipient, new_keys):
        to_send = {
            "worker_num": self.worker_num,
            "recipient": recipient,
            "type": "2PC_NEW_KEY",
            "new_keys": new_keys,
            "start_value": self.next_value,
        }
        return to_send

    def make_2PC_VOTE(self, recipient, received, yes_or_no):
        assert received["type"] == "2PC_NEW_KEY"
        to_send = {
            "worker_num": self.worker_num,
            "recipient": recipient,
            "type": "2PC_VOTE",
            "vote": "VOTE_" + yes_or_no,
            "new_keys": received["new_keys"],
            "start_value": received["start_value"],
        }
        return to_send
    def make_2PC_VOTE_NO(self, recipient, received):
        return self.make_2PC_VOTE(recipient, received, "NO")
    def make_2PC_VOTE_YES(self, recipient, received):
        return self.make_2PC_VOTE(recipient, received, "YES")

    def make_2PC_COMPLETION(self, recipient, commit_or_abort, new_keys, start_value):
        to_send = {
            "worker_num": self.worker_num,
            "recipient": recipient,
            "type": "2PC_COMPLETION",
            "result": commit_or_abort,
            "new_keys": new_keys,
            "start_value": start_value,
        }
        return to_send
    def make_2PC_COMPLETION_COMMIT(self, recipient, new_keys, start_value):
        return self.make_2PC_COMPLETION(recipient, "COMMIT", new_keys, start_value)
    def make_2PC_COMPLETION_ABORT(self, recipient, new_keys, start_value):
        return self.make_2PC_COMPLETION(recipient, "ABORT", new_keys, start_value)

    def make_2PC_DONE(self, recipient):
        to_send = {
            "worker_num": self.worker_num,
            "recipient": recipient,
            "type": "2PC_DONE",
        }
        return to_send


    def send_NEW_KEYs(self, new_keys):
        for i in range(len(self.pipes)):
            p = self.pipes[i]
            recipient_num = i if i < self.worker_num else i+1
            self.my_send(p, self.make_2PC_NEW_KEY(recipient_num, new_keys))
        self.sch.run()

    def send_ABORTs(self, new_keys, start_value):
        self.abort += 1
        for i in range(len(self.pipes)):
            p = self.pipes[i]
            recipient_num = i if i < self.worker_num else i+1
            self.my_send(p, self.make_2PC_COMPLETION_ABORT(recipient_num, new_keys, start_value))
        self.sch.run()

    def send_COMMITs(self, new_keys, start_value):
        for i in range(len(self.pipes)):
            p = self.pipes[i]
            recipient_num = i if i < self.worker_num else i+1
            self.my_send(p, self.make_2PC_COMPLETION_COMMIT(recipient_num, new_keys, start_value))
        self.sch.run()

    def send_DONEs(self):
        #self.my_send(self.manager_pipe, None)
        for i in range(len(self.pipes)):
            p = self.pipes[i]
            recipient_num = i if i < self.worker_num else i+1
            self.my_send(p, self.make_2PC_DONE(recipient_num))
        self.my_send(self.manager_pipe, self.make_2PC_DONE('VALUE_SPACE_MANAGER'))
        self.sch.run()

    def apply_new_value_space(self):
        self.my_send(self.manager_pipe, self.make_APPLY_VALUE_SPACE())
        self.sch.run()
        while (self.manager_pipe.poll(None)):
            received = self.manager_pipe.recv()
            self.alloc = received["new_start_value"]
            self.next_value = self.alloc
            #print("worker {} receive new value {}").format(self.worker_num, self.next_value)
            break


    def wait_for_COMPLETION(self, bully_num, bully_pipe):

        while True:
            received = bully_pipe.recv()

            # Don't look at irrelevant messages
            if received["type"] != "2PC_COMPLETION":
                continue

            if received["result"] == "COMMIT":
                for k, v in received["new_keys"].items():
                    self.d[k] = v
                return True
            return False

    def handle_COMPLETION(self, received):
        if received["result"] == "COMMIT":
            for k, v in received["new_keys"].items():
                self.d[k] = v
            return True
        # Otherwise, the bully got bullied. Need to restart the handling.
        return False

    def handle_new_values_2PC_bully(self):
        done = False
        while not done:
            done = True
            cont = False
            bully_num = None
            bully_msg = None
            bully_pipe = None
            temp_dict = {}
            for p in self.pipes:
                # We only ever care about the most recent NEW_CK message from a process.
                # Check if it is for next_value and, if there are such messages from multiple
                # workers, take the one from the "bully" (highest numbered worker).
                while p.poll():
                    save = True
                    received = p.recv()

                    sender = received["worker_num"]
                    # If 2PC_DONE, note that the worker will not be suggesting any more new keys

                    if received["type"] == "2PC_DONE":
                        self.done.append(received["worker_num"])
                    elif received["type"] == "2PC_COMPLETION":
                        # This better be an old ABORT. Ignore it.
                        # assert received["result"] == "ABORT"
                        self.handle_COMPLETION(received)
                        self.saved_proposal[sender][0] = None
                        self.saved_proposal[sender][1] = None
                    elif received["type"] == "2PC_VOTE":
                        # This is from when we were a coordinator. Ignore it.
                        pass
                    else:
                        # This is what we care about. We want a NEW_KEY with
                        assert received["type"] == "2PC_NEW_KEY"
                        # If this is for next_value, potentially use it
                        for k, v in received["new_keys"].items():
                            if k in self.d:
                                self.my_send(p, self.make_2PC_VOTE_NO(sender, received))
                                self.sch.run()
                                save = False
                                self.key_conflict += 1
                                break

                        if save == True:
                            self.saved_proposal[sender][0] = received
                            self.saved_proposal[sender][1] = p
                            self.my_send(p, self.make_2PC_VOTE_YES(sender, received))
                            self.sch.run()
                        else:
                            # This should already be ABORTed.
                            pass
            # Wait all new key-value proposal committed/aborted until move forward
            for w in range(len(self.pipes)+1):
                if self.saved_proposal[w][0] != None and self.saved_proposal[w][1] != None:
                    self.wait_for_COMPLETION(w, self.saved_proposal[w][1])
                    self.saved_proposal[w][0] = None
                    self.saved_proposal[w][1] = None


    def propose_new_keys_2PC_bully(self, new_keys_list):
        # Continue until value is committed
        pre_next_val = self.next_value
        pre_alloc = self.alloc
        start_over = True
        while start_over:
            # Take all the keys from new_key_list that aren't in self.d
            # and add them to new_keys with their proposed value.
            new_keys = {}
            val = pre_next_val
            for k in new_keys_list:
                if k not in self.d:
                    new_keys[k] = val
                    val += 1
                    if val - pre_alloc == NUMPERALLOC:
                        if pre_alloc == self.alloc:
                            self.apply_new_value_space()
                        val = self.next_value
            if len(new_keys) == 0:
                return

            start_over = False

            self.send_NEW_KEYs(new_keys)
            for w in range(len(self.pipes)+1):
                assert self.saved_proposal[w][0] == None and self.saved_proposal[w][1] == None
            start = timer()
            voted_yes = []
            while len(voted_yes) < len(self.pipes):
                elaspe = timer()
                timeout = elaspe -start
                # timeout to avoid deadlock
                if timeout > MAXWAITING:
                    self.send_ABORTs(new_keys, self.next_value)
                    start_over = True
                    break
                for p in self.pipes:
                    # Don't look at threads that have already voted yes
                    if p in voted_yes:
                        continue

                    while p.poll():
                        # Want to recv() until we get a response to our most recent message
                        received = p.recv()
                        sender = received["worker_num"]

                        if received["type"] == "2PC_DONE":
                            self.done.append(sender)
                            continue

                        if received["type"] == "2PC_COMPLETION":
                            # Delete proposer when completed
                            self.handle_COMPLETION(received)
                            self.saved_proposal[sender][0] = None
                            self.saved_proposal[sender][1] = None

                        if received["type"] == "2PC_VOTE" and received["new_keys"] == new_keys and received["start_value"] == self.next_value:
                            if received["vote"] == "VOTE_YES":
                                # Got what we wanted
                                voted_yes.append(p)
                                break
                            else:
                                # Got a VOTE_NO for the current proposal, must ABORT and start over
                                self.send_ABORTs(new_keys, self.next_value)
                                start_over = True
                                break

                        if received["type"] == "2PC_NEW_KEY":
                            confli = False
                            for k, v in received["new_keys"].items():
                                if k in self.d:
                                    self.my_send(p, self.make_2PC_VOTE_NO(sender, received))
                                    self.sch.run()
                                    confli = True
                                    self.key_conflict += 1
                                    break
                                if k in new_keys.keys():
                                    confli = True
                                    self.key_conflict += 1
                                    if sender > self.worker_num:
                                        # Save the proposer and wait for commit/ abort in future when vote yes
                                        self.my_send(p, self.make_2PC_VOTE_YES(sender, received))
                                        self.sch.run()
                                        self.saved_proposal[sender][0] = received
                                        self.saved_proposal[sender][1] = p
                                        # Be bullied
                                        self.send_ABORTs(new_keys, self.next_value)
                                        start_over = True
                                        break
                                    else:
                                        self.my_send(p, self.make_2PC_VOTE_NO(sender, received))
                                        self.sch.run()
                                        break
                                else:
                                    pass
                            if confli == False:
                                # Save the proposer and wait for commit/ abort in future when vote yes
                                self.my_send(p, self.make_2PC_VOTE_YES(sender, received))
                                self.sch.run()
                                self.saved_proposal[sender][0] = received
                                self.saved_proposal[sender][1] = p
                        # If we get here, this is some other old message; we don't care about it.
                    if start_over:
                        break
                if start_over:
                    break
            if not start_over:
                # We made it!
                for k, v in new_keys.items():
                    self.d[k] = v
                self.send_COMMITs(new_keys, self.next_value)
                end = timer()
                #print end-start
                self.next_value = val if val>self.alloc else self.alloc
                #print len(new_keys)
                # Others don't send ACKs since we assume no failues
            else:
                # We're behind... Need to handle new values and retry
                self.handle_new_values_2PC_bully()

    def run(self):
        new_keys_list = []
        self.apply_new_value_space()
        with open(self.file) as f:
            for line in f:
                # loop through all connections
                self.handle_new_values_2PC_bully()

                line = line.strip()

                key = line.split(",")[self.fieldnum]
                if key not in self.d and key not in new_keys_list:
                    new_keys_list.append(key)
                    if len(new_keys_list) == self.buffer_size:
                        self.propose_new_keys_2PC_bully(new_keys_list)
                        new_keys_list = []
        if len(new_keys_list) > 0:
            self.propose_new_keys_2PC_bully(new_keys_list)
        # Done parsing, so no longer sending new proposals
        self.send_DONEs()
        self.manager_pipe.close()
        while len(self.done) < len(self.pipes):
            self.handle_new_values_2PC_bully()
        for p in self.pipes:
            p.close()
        # print len(self.d)
        return self.sent, self.key_conflict, self.abort
