class FlightComputer:
    def __init__(self, state):
        self.state = state
        self.current_stage_index = 0
        self.completed = True
        self.stage_handlers = [
            self._handle_stage_1,
            self._handle_stage_2,
            self._handle_stage_3,
            self._handle_stage_4,
            self._handle_stage_5,
            self._handle_stage_6,
            self._handle_stage_7,
            self._handle_stage_8,
            self._handle_stage_9]
        self.stage_handler = self.stage_handlers[self.current_stage_index]

    def start(self):
        pass

    def add_peers(self, peers):
        pass

    def decide_on_state(self, state):
        decided = self.acceptable_state(state)
        self.deliver_state(state)
        return decided

    def decide_on_action(self, action):
        decided = self.acceptable_action(action)
        self.deliver_action(action)
        return decided

    def sample_next_action(self):
        return self.stage_handler()

    def acceptable_state(self, state):
        return True

    def acceptable_action(self, action):
        our_action = self.sample_next_action()
        our_keys = set(our_action.keys())
        keys = set(action.keys())
        if our_keys > keys or our_keys < keys:
            return False
        for k in our_action.keys():
            if our_action[k] != action[k]:
                return False

        return True

    def deliver_action(self, action):
        if "next_stage" in action and action["next_stage"]:
            self.current_stage_index += 1
            self.stage_handler = self.stage_handlers[self.current_stage_index]

    def deliver_state(self, state):
        self.state = state

    def _handle_stage_1(self):
        action = {"pitch": 90, "throttle": 1.0, "heading": 90, "stage": False, "next_state": False}
        if self.state["altitude"] >= 1000:
            action["pitch"] = 80
            action["next_stage"] = True

        return action

    def _handle_stage_2(self):
        action = {"pitch": 80, "throttle": 1.0, "heading": 90, "stage": False, "next_state": False}
        # Eject SRB's before the gravity turn
        if self.state["fuel_srb"] <= 1250:
            action["stage"] = True
            action["next_stage"] = True

        return action

    def _handle_stage_3(self):
        action = {"pitch": 80, "throttle": 1.0, "heading": 90, "stage": False, "next_state": False}
        # Eject 2nd SRB + Initial Gravity turn
        if self.state["fuel_srb"] <= 10:
            action["stage"] = True
            action["pitch"] = 60.0
            action["next_stage"] = True

        return action

    def _handle_stage_4(self):
        action = {"pitch": 80, "throttle": 1.0, "heading": 90, "stage": False, "next_state": False}
        # Turn
        if self.state["altitude"] >= 25000:
            action["pitch"] = 0
            action["throttle"] = 0.75
            action["next_stage"] = True

        return action

    def _handle_stage_5(self):
        action = {"pitch": 0, "throttle": 0.75, "heading": 90, "stage": False, "next_state": False}
        # Cut throttle when apoapsis is 100km
        if self.state["apoapsis"] >= 100000:
            action["throttle"] = 0.0
            action["next_stage"] = True

        return action

    def _handle_stage_6(self):
        action = {"pitch": 0, "throttle": 0.0, "heading": 90, "stage": False, "next_state": False}
        # Drop stage
        if self.state["altitude"] >= 80000:
            action["stage"] = True
            action["next_stage"] = True

        return action

    def _handle_stage_7(self):
        action = {"pitch": 0, "throttle": 0.0, "heading": 90, "stage": False, "next_state": False}
        # Poor man's circularisation
        if self.state["altitude"] >= 100000:
            action["throttle"] = 1.0
            action["next_stage"] = True

        return action

    def _handle_stage_8(self):
        action = {"pitch": 0, "throttle": 1.0, "heading": 90, "stage": False, "next_state": False}
        if self.state["periapsis"] >= 90000:
            action["throttle"] = 0.0
            action["next_stage"] = True

        return action

    def _handle_stage_9(self):
        self.completed = True
