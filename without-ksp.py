import argparse
import math
import pickle
import time
import random
import traceback

from computer import CooperatingComputer, allocate_faulty_flight_computer

# Load the pickle files
actions = pickle.load(open("data/actions.pickle", "rb"))
states = pickle.load(open("data/states.pickle", "rb"))
timestep = 0

# Argument parsing
parser = argparse.ArgumentParser()
parser.add_argument("--correct-fraction", type=float, default=1.0, help="Fraction of correct flight computers (default 1.0).")
parser.add_argument("--flight-computers", type=int, default=3, help="Number of flight computers (default: 3).")
arguments, _ = parser.parse_known_args()


def readout_state():
    return states[timestep]


def execute_action(action):
    print(action)
    print(actions[timestep])
    keys = set(action.keys())
    correct_keys = set(actions[timestep])
    assert(not(keys < correct_keys or keys > correct_keys)) 
    for k in action.keys():
        assert(action[k] == actions[timestep][k])


def allocate_flight_computers(arguments):
    flight_computers = []
    n_fc = arguments.flight_computers
    n_correct_fc = math.ceil(arguments.correct_fraction * n_fc)
    n_incorrect_fc = n_fc - n_correct_fc
    state = readout_state()
    for i in range(n_correct_fc):
        flight_computers.append(CooperatingComputer(state, i))
    for i in range(n_incorrect_fc):
        flight_computers.append(allocate_faulty_flight_computer(state, i + n_correct_fc))
    # Add the peers for the consensus protocol
    for fc in flight_computers:
        for peer in flight_computers:
            if fc != peer:
                fc.add_peers(peer)
    for fc in flight_computers:
        fc.start()


    return flight_computers

# Connect with Kerbal Space Program
flight_computers = allocate_flight_computers(arguments)
alive_flight_computers = [*flight_computers]

def select_leader():
    return random.choice(alive_flight_computers)


def next_action(state):
    leader = select_leader()
    state_decided = leader.decide_on_state(state)
    if not state_decided:
        return None
    action = leader.sample_next_action()
    action_decided = leader.decide_on_action(action)
    if action_decided:
        return action

    return None

complete = False
try:
    while not complete:
        print(timestep)
        timestep += 1
        state = readout_state()
        leader = select_leader()
        state_decided = leader.decide_on_state(state)
        if not state_decided:
            alive_flight_computers.remove(leader)
            continue

        action = leader.sample_next_action()
        if action is None:
            complete = True
            continue

        action_decided = leader.decide_on_action(action)
        if action_decided:
            execute_action(action)
        else:
            alive_flight_computers.remove(leader)
            timestep -= 1

except Exception as e:
    print(e)
    traceback.print_exc()

if complete:
    print("Success!")
else:
    print("Fail!")

for fc in flight_computers:
    fc.stop()
