from basic_abstraction.link import FairLossLink
from basic_abstraction.consensus import HierarchicalConsensus
from basic_abstraction.broadcast import EagerReliableBroadcast
from computer.base import FlightComputer
import time
from basic_abstraction.utils import WorkerThread

class CooperatingComputer(FlightComputer):
    def __init__(self, state, process_number):
        super().__init__(state)
        self.process_number = process_number
        self.peers = set()

        self.link = FairLossLink(process_number)
        self.consensus = HierarchicalConsensus(self.link)
        self.consensus.debug = True
        self.broadcast = EagerReliableBroadcast(self.link, self.broadcast_receive)
        self.worker_thread = WorkerThread()
        self.worker_thread.start()


    def add_peers(self, *peers):
        peers_number = [peer.process_number for peer in peers]
        self.peers.update(peers_number)
        self.consensus.add_peers(*peers_number)
        self.broadcast.add_peers(*peers_number)

    def start(self):
        self.consensus.start()

    def kill(self):
        self.worker_thread.kill()
        self.consensus.kill()
        self.link.kill()

    def broadcast_receive(self, source, raw_message):
        message_type, message = raw_message
        if source != self.process_number:
            if message_type == "state":
                self.node_decide_on_state(message)
            elif message_type == "action":
                self.node_decide_on_action(message)

    def decide_on_state(self, state):
        print(f"{self.link.process_number}: Je renverse l'État")
        self.broadcast.broadcast(("state", state))
        # TODO organise a vote
        self.node_decide_on_state(state)

        return self.wait_node_decide_on_state(state)

    def node_decide_on_state(self, state):
        self.consensus.propose(self.acceptable_state(state))

        self.worker_thread.put(self.wait_node_decide_on_state, (state,))

    def wait_node_decide_on_state(self, state):
        decided = self.consensus.get_decided()
        if decided:
            self.deliver_state(state)

        return decided

    def decide_on_action(self, action):
        print(f"{self.link.process_number}: Je démarre l'action")
        self.broadcast.broadcast(("action", action))
        # TODO organise a vote
        self.node_decide_on_action(action)
        return self.wait_node_decide_on_action(action)

    def node_decide_on_action(self, action):
        self.consensus.propose(self.acceptable_action(action))

        self.worker_thread.put(self.wait_node_decide_on_action, (action,))

    def wait_node_decide_on_action(self, action):
        decided = self.consensus.get_decided()
        if decided:
            self.deliver_action(action)

        return decided
