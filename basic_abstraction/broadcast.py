from basic_abstraction.link import FairLossLink

class Broadcast:
    def __init__(self, link, deliver_callback):
        self.link = link
        self.deliver_callback = deliver_callback
        self.debug = False

    def broadcast(self, message):
        pass

    def add_peers(self, *peers):
        pass


class BestEffortBroadcast(Broadcast):
    message_type = "beb"

    def __init__(self, link, deliver_callback, loss=0.0):
        super().__init__(link, deliver_callback)
        self.link.add_callback(self.receive)
        self.peers = set()
        
    def add_peers(self, *peers):
        self.peers.update(peers)

    def broadcast(self, message):
        if self.debug:
            print(f"{self.link.process_number}(beb): {message} broadcasted.")
        for peer in self.peers:
            self.link.send(peer, (self.message_type, message))

    def receive(self, source_number, raw_message):
        if raw_message[0] == self.message_type:
            message = raw_message[1]
            self.deliver_callback(source_number, message)
            if self.debug:
                print(f"{self.link.process_number}(beb): {message} received.")


class EagerReliableBroadcast(Broadcast):
    message_type = "erb"

    def __init__(self, link, deliver_callback, loss=0.0, max_concurrent_messages=20):
        super().__init__(link, deliver_callback)
        self.be_broadcast = BestEffortBroadcast(link, self.be_deliver, loss)
        self.delivered = [None] * max_concurrent_messages
        self.delivered_cycle = 0
        self.sequence_number = 0

    def add_peers(self, *peers):
        self.be_broadcast.add_peers(*peers)

    def register_delivered(self, raw_message):
        self.delivered[self.delivered_cycle] = raw_message
        self.delivered_cycle = (self.delivered_cycle + 1) % len(self.delivered)

    def broadcast(self, message):
        raw_message = (self.message_type, self.link.process_number, message, self.next_sequence_number())
        if self.debug:
            print(f"{self.link.process_number}(erb): {raw_message[2:-1]} broadcasted.")
        self.register_delivered(raw_message)
        self.be_broadcast.broadcast(raw_message)

    def next_sequence_number(self):
        self.sequence_number += 1
        return self.sequence_number

    def be_deliver(self, source_number, raw_message):
        if raw_message[0] == self.message_type:
            _, source, message, sequence = raw_message
            if raw_message not in self.delivered:
                self.register_delivered(raw_message)
                self.be_broadcast.broadcast(raw_message)
                if self.debug:
                    print(f"{self.link.process_number}(erb): {(message, sequence)} from {source}.")
                self.deliver_callback(source, message)


if (__name__ == "__main__"):
    import time

    class Test:
        def __init__(self, process_number):
            self.process_number = process_number
            self.link = FairLossLink(process_number)
            self.beb = EagerReliableBroadcast(self.link, self.deliver)

        def deliver(self, source_number, message):
            print("{}: Message received {} from {}".format(self.process_number, message, source_number))

        def kill(self):
            self.link.kill()

    test0 = Test(0)
    test1 = Test(1)
    test2 = Test(2)

    test0.beb.add_peers(0,1,2)
    test1.beb.add_peers(0,1,2)
    test2.beb.add_peers(0,1,2)

    test0.beb.broadcast("Hello world !")
    test2.beb.broadcast("Hello back")

    time.sleep(0.1)

    test0.kill()
    test1.kill()
    test2.kill()

