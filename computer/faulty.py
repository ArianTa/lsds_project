import random
import time

from basic_abstraction.base import Abstraction
from basic_abstraction.link import PerfectLink
from basic_abstraction.failure_detectors import PerfectFailureDetector
from basic_abstraction.broadcast import BestEffortBroadcast, EagerReliableBroadcast
from basic_abstraction.consensus import HierarchicalConsensus

from .cooperating import CooperatingComputer

from basic_abstraction import PerfectLink

class FullThrottleFlightComputer(CooperatingComputer):

    def __init__(self, state, process_number):
        super(FullThrottleFlightComputer, self).__init__(state, process_number)

    def sample_next_action(self):
        action = super(FullThrottleFlightComputer, self).sample_next_action()
        action["throttle"] = 1.0
        return action


class RandomThrottleFlightComputer(CooperatingComputer):

    def __init__(self, state, process_number):
        super(RandomThrottleFlightComputer, self).__init__(state, process_number)

    def sample_next_action(self):
        action = super(RandomThrottleFlightComputer, self).sample_next_action()
        action["throttle"] = random.uniform(0, 1)

        return action


class SlowFlightComputer(CooperatingComputer):

    class SlowPerfectLink(PerfectLink):
        def send(self, *args, **kwargs):
            time.sleep(random.uniform(1, 10)) # Seconds
            super().send(*args, **kwargs)

    def __init__(self, state, process_number):
        super(SlowFlightComputer, self).__init__(state, process_number)
        mv = self.majority_voting
        mv.link = SlowFlightComputer.SlowPerfectLink(process_number)
        mv.pfd = PerfectFailureDetector(mv.link)
        mv.pfd.register(mv, mv.peer_failure)
        mv.erb = EagerReliableBroadcast(mv.link)
        mv.broadcast = mv.erb.register(mv)
        mv.beb = BestEffortBroadcast(mv.link)
        mv.hco = HierarchicalConsensus(mv.link, mv.pfd, mv.beb, mv, mv.consensus_decided)


class CrashingFlightComputer(CooperatingComputer):

    def __init__(self, state, process_number):
        super(CrashingFlightComputer, self).__init__(state, process_number)

    def sample_next_action(self):
        action = super(CrashingFlightComputer, self).sample_next_action()
        # 1% probability of a crash
        if random.uniform(0, 1) <= 0.01:
            self.majority_voting.stop()

        return action



def allocate_faulty_flight_computer(state, process_number):
    computers = [
        FullThrottleFlightComputer,
        RandomThrottleFlightComputer,
        SlowFlightComputer,
        CrashingFlightComputer,
    ]
    return random.choice(computers)(state, process_number)