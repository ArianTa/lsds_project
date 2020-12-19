from basic_abstraction.broadcast import BestEffortBroadcast
from basic_abstraction.failure_detectors import PerfectFailureDetector
from basic_abstraction.link import FairLossLink
import time
import threading

class Consensus:
    def __init__(self, link):
        self.link = link
        self.debug = False

    def add_peers(peers_number_list):
        pass

    def propose(self, value):
        return value

class HierarchicalConsensus(Consensus):
    def __init__(self, link):
        super().__init__(link)
        self.broadcast = BestEffortBroadcast(link, self.broadcast_receive)
        self.failure_detector = PerfectFailureDetector(self.link, self.failure_detection, timeout_time=1)
        self.peers = {self.link.process_number}
        self.alive = True

        self.failed_nodes = set()
        self.lock = threading.RLock()
        self.decided_event = threading.Event()
        self.decided = None
        self.reset()

    def reset(self):
        if self.debug:
            print(f"{self.link.process_number}(c): Resetting consensus.")
        self.round = 0
        self.proposal = None
        self.proposer = -1
        self.delivered = {peer: False for peer in self.peers}
        self.broadcasting = False

    def add_peers(self, *peers):
        self.broadcast.add_peers(*peers)
        self.failure_detector.add_peers(*peers)
        self.peers.update(peers)
        self.delivered.update({peer: False for peer in peers})

    def failure_detection(self, process_number):
        with self.lock:
            if self.debug:
                print(f"{self.link.process_number}(c): {process_number} crash detected.")
            self.failed_nodes.add(process_number)
        self.update()

    def start(self):
        self.failure_detector.start_heartbeat()

    def update(self):
        with self.lock:
            if self.round < len(self.peers):
                if self.round in self.failed_nodes or self.delivered[self.round]:
                    self.round += 1
                    self.update()

                elif self.round == self.link.process_number and not self.decided_event.is_set():
                    if self.debug:
                        print(f"{self.link.process_number}(c): Decided on {self.proposal} and broadcasting.")
                    self.broadcast.broadcast(("c", self.proposal))
                    self.decided = self.proposal
                    self.round += 1
                    self.update()
            else:
                self.decided_event.set()
                self.reset()

    def propose(self, value):
        if not self.alive or self.decided is not None:
            return False


        if self.debug:
            print(f"{self.link.process_number}(c): Got proposition {value}.")

        with self.lock:
            if self.proposal is None:
                self.proposal = value

            self.decided_event.clear()

        self.update()
        
        return True

    def wait_decided(self):
        self.decided_event.wait()
        return self.get_decided()

    def get_decided(self):
        if self.decided_event.is_set():
            decided = self.decided
            self.decided = None
            return decided
        return None

    def broadcast_receive(self, source_number, raw_message):
        mess_type = raw_message[0]
        if mess_type == "c":
            value = raw_message[1]
            if self.debug:
                print(f"{self.link.process_number}(c): {source_number} has decided on {value}.")
            if source_number < self.link.process_number and source_number > self.proposer:
                self.proposal = value
                self.proposer = source_number
            self.delivered[source_number] = True
            self.update()


    def kill(self):
        if self.debug:
            print(f"{self.link.process_number}(c): Killing consensus.")
        self.alive = False
        self.failure_detector.kill()


if __name__ == "__main__":
    import threading

    class Test:
        def __init__(self, process_number):
            self.process_number = process_number
            self.link = FairLossLink(process_number)
            self.consensus = HierarchicalConsensus(self.link)
            self.proposal = None

        def propose(self, value):
            return self.consensus.propose(value)

        def decided(self):
            print("{}: Decided on {}".format(self.process_number, self.consensus.wait_decided()))

        def kill(self):
            self.consensus.kill()
            self.link.kill()



    test0 = Test(0)
    test1 = Test(1)
    test1.consensus.debug = True
    test2 = Test(2)

    test0.consensus.add_peers(1, 2)
    test1.consensus.add_peers(0, 2)
    test2.consensus.add_peers(1, 0)

    test0.consensus.start()
    test1.consensus.start()
    test2.consensus.start()

    test0.propose("lol0")
    test1.propose("lil0")
    test2.propose("wesh0")

    test0.decided()
    test1.decided()
    test2.decided()

    time.sleep(0.2)

    test1.kill()

    test0.propose("lol1")
    test1.propose("lil1")
    test2.propose("wesh1")

    test0.decided()
    test1.decided()
    test2.decided()

    time.sleep(0.2)

    test0.kill()
    test2.kill()
