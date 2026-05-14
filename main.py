from raft import Redis, Command, Op, Role
import time

if __name__ == "__main__":
    redis = Redis()
    time.sleep(0.5)
    for node in redis.nodes:
        print(node.role, node.kv)

    command = Command(Op.GET, 1, "Tyler")
    redis.send(command)

    for node in redis.nodes:
        if node.role == Role.LEADER:
            print(node.log)
    