from raft import Raft
import time

if __name__ == "__main__":
    raft = Raft()
    time.sleep(1)
    for node in raft.nodes:
        print(node.role, node.term)
    