from enum import Enum
import threading
import random
import time

class Command:
    def __init__(self, op, key, value=None):
        self.op = op
        self.key = key
        self.value = value
        self.status = LogStatus.APPENDED

    def __repr__(self):
        return (
            f"Command(op={self.op!r}, "
            f"key={self.key!r}, "
            f"value={self.value!r}), "
            f"status={self.status!r}"
        )

class Redis:
    def __init__(self, num_nodes=5):
        self.num_nodes = num_nodes
        self.threads = []

        self.nodes = [Node() for _ in range(self.num_nodes)]
        for node in self.nodes:
            node.peers = [n for n in self.nodes if n is not node]

        self.start()
        self.status = NodeStatus.HEALTHY

    def start(self):
        self.threads = [
            threading.Thread(target=node.run) for node in self.nodes
        ]
        for thread in self.threads:
            thread.start()

    def stop(self):
        for node in self.nodes:
            node.running = False

        for thread in self.threads:
            thread.join() 

    def send(self, command):
        for node in self.nodes:
            node.receive_client_command(command)    

class Node:
    def __init__(self):
        self.role = Role.FOLLOWER
        self.status = NodeStatus.HEALTHY

        # Log and peers
        self.log_index = 0
        self.log = []
        self.peers = []
        self.kv = {}

        # Voting
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

    def receive_client_command(self, command):
        if self.role is not Role.LEADER:
            return False

        self.log.append(command)
        # replicate entry to followers
        # once a majority have the command, mark is committed
        # each node applies the log entry
        # once all nodes apply the entry, mark it applied
    
    def receive_leader_command(self, command):
        pass

    def get(self, command):
        pass

    def put(self, command):
        pass

class Role(Enum):
    LEADER = 1
    CANDIDATE = 2
    FOLLOWER = 3

class NodeStatus(Enum):
    HEALTHY = 1
    DOWN = 2
    STALE = 3
    LAGGING = 4
    PARTITIONED = 5

class LogStatus(Enum):
    APPENDED = 1
    COMMITTED = 2
    APPLIED = 3

class Op(Enum):
    GET = 1
    PUT = 2

def send_vote_rpc(voter_node, candidate_term, candidate_log_index):
    if ((candidate_term > voter_node.term) or ((candidate_term == voter_node.term) and (candidate_log_index >= voter_node.log_index))) and not voter_node.has_voted:
        voter_node.has_voted = True
        return 1
    return 0

def is_majority(vote_count, total_nodes):
    return vote_count > (total_nodes / 2)