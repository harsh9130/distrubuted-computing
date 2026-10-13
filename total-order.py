import argparse
import heapq
import random


class Network:
    """Discrete-event simulator with FIFO channels and random delays."""

    def __init__(self, n):
        self.time = 0
        self.events = []           # heap of (time, seq, callback)
        self.seq = 0
        self.last = {}             # (src, dst) -> last arrival time (keeps FIFO)
        self.n = n
        self.procs = []

    def schedule(self, t, fn):
        self.seq += 1
        heapq.heappush(self.events, (t, self.seq, fn))

    def send(self, src, dst, kind, data):
        arrive = max(self.time + random.randint(1, 10),
                     self.last.get((src, dst), 0) + 1)   # FIFO per channel
        self.last[(src, dst)] = arrive
        self.schedule(arrive, lambda: self.procs[dst].receive(src, kind, data))

    def broadcast(self, src, kind, data):
        for dst in range(self.n):          # includes the sender itself
            self.send(src, dst, kind, data)

    def run(self):
        while self.events:
            self.time, _, fn = heapq.heappop(self.events)
            fn()


class Process:
    def __init__(self, pid, net):
        self.pid, self.net = pid, net
        self.clock = 0
        self.queue = []            # heap of (ts, sender, payload)
        self.acks = {}             # (ts, sender) -> set of ackers
        self.delivered = []

    def multicast(self, payload):
        self.clock += 1
        msg = (self.clock, self.pid, payload)
        print(f"t={self.net.time:3d}  P{self.pid} multicasts {payload!r} ts={msg[0]}")
        self.net.broadcast(self.pid, "MSG", msg)

    def receive(self, src, kind, data):
        if kind == "MSG":
            ts, sender, _ = data
            self.clock = max(self.clock, ts) + 1
            heapq.heappush(self.queue, data)
            self.net.broadcast(self.pid, "ACK", (ts, sender))
        else:  # ACK
            self.acks.setdefault(data, set()).add(src)
        self.try_deliver()

    def try_deliver(self):
        while self.queue:
            ts, sender, payload = self.queue[0]
            if len(self.acks.get((ts, sender), ())) < self.net.n:
                break
            heapq.heappop(self.queue)
            self.delivered.append(payload)
            print(f"t={self.net.time:3d}  P{self.pid} delivers   {payload!r} (ts={ts}, from P{sender})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", type=int, default=3, help="number of processes")
    ap.add_argument("-m", type=int, default=6, help="number of multicasts")
    ap.add_argument("-s", type=int, default=None, help="random seed")
    a = ap.parse_args()
    random.seed(a.s)

    net = Network(a.n)
    net.procs = [Process(i, net) for i in range(a.n)]

    # schedule random multicasts from random processes at random times
    for k in range(a.m):
        p = random.randrange(a.n)
        net.schedule(random.randint(0, 15),
                     lambda p=p, k=k: net.procs[p].multicast(f"m{k}"))
    net.run()

    print("\n=== Delivery order per process ===")
    for p in net.procs:
        print(f"P{p.pid}: {p.delivered}")
    same = all(p.delivered == net.procs[0].delivered for p in net.procs)
    print("\nTotal order preserved:", "YES" if same else "NO")


if __name__ == "__main__":
    main()