from enum import Enum
import threading
import random
import time

class Raft:
    def __init__(self, num_nodes=5):
        self.num_nodes = num_nodes

        self.nodes = [Node() for _ in range(self.num_nodes)]
        for node in self.nodes:
            node.peers = [n for n in self.nodes if n is not node]

        self.start()
        self.status = Status.HEALTHY

    def start(self):
        for node in self.nodes:
            threading.Thread(target=node.run).start()

class Node:
    def __init__(self):
        self.role = Role.FOLLOWER
        self.kv = {}
        self.status = Status.HEALTHY
        self.log_index = 0
        self.peers = []
        self.term = 1
        self.has_voted = False
        self.running = False
        self.election_deadline = None
        self.lock = threading.Lock()

    def run(self):
        self.running = True
        self.reset_election_timer()

        while self.running:
            time.sleep(0.01)
            
            if self.role == Role.LEADER:
                for node in self.peers:
                    node.receive_heartbeats(self.term)
            
            with self.lock:
                timed_out = (
                    self.role != Role.LEADER
                    and time.monotonic() >= self.election_deadline
                )

            if timed_out:
                self.request_votes()
                self.reset_election_timer()

    def request_votes(self):
        self.role = Role.CANDIDATE
        self.term += 1
        vote_count = 1 # node votes for itself
        self.has_voted = True

        # send RPC to other nodes
        for node in self.peers:
            if node is not self:
                vote_count += send_vote_rpc(node, self.term, self.log_index)

        if is_majority(vote_count, len(self.peers) + 1):
            self.role = Role.LEADER
        else:
            self.role = Role.FOLLOWER

    def reset_election_timer(self):
        self.election_deadline = time.monotonic() + random.uniform(0.15, 0.30)

    def receive_heartbeats(self, leader_term):
        with self.lock:
            if leader_term >= self.term:
                self.term = leader_term
                self.role = Role.FOLLOWER
                self.reset_election_timer()

class Role(Enum):
    LEADER = 1
    CANDIDATE = 2
    FOLLOWER = 3

class Status(Enum):
    HEALTHY = 1
    DOWN = 2
    STALE = 3
    LAGGING = 4
    PARTITIONED = 5

def send_vote_rpc(voter_node, candidate_term, candidate_log_index):
    if ((candidate_term > voter_node.term) or ((candidate_term == voter_node.term) and (candidate_log_index >= voter_node.log_index))) and not voter_node.has_voted:
        voter_node.has_voted = True
        return 1
    return 0

def is_majority(vote_count, total_nodes):
    return vote_count > (total_nodes / 2)