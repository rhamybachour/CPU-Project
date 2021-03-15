"""
Run some bank teller queue processing simulations.
"""

from desimul import Calendar, Event, Server
from queue import Queue
from abc import ABCMeta, abstractmethod
import random


# Auxiliary classes

class Client:
    """
    Implements methods to verify waiting times for each client.
    """

    def __init__(self, work):
        self.__arrival_time = None
        self.__departure_time = None
        self.__total_work = work

    def arrival(self, time):
        """Called when client arrives."""
        self.__arrival_time = time

    def departure(self, time):
        """Called when client departs."""
        self.__departure_time = time

    def report(self):
        """Report on arrival and departure times (for statistics)."""
        return self.__arrival_time, self.__departure_time

    def work(self):
        return self.__total_work


# Server classes

class QueueingSystem(Server,metaclass=ABCMeta):
    """
    Base class for all client queueing systems.
    """

    def __init__(self, calendar):
        Server.__init__(self, calendar)
        self.__free_tellers = Queue()

    def new_client(self, client):
        """A new client to attend. Either send it to a free teller (if available) or to waiting queue."""
        if self.__free_tellers.empty():
            self.enqueue(client)
        else:
            teller = self.__free_tellers.get()
            cal = self.calendar()
            now = cal.current_time()
            event = ClientToTellerEvent(now, teller, client)
            cal.put(event)

    def free_teller(self, teller):
        """A new free teller. Send it a client (if available) or put in waiting queue."""
        if self.has_waiting_client():
            client = self.get_next_client()
            cal = self.calendar()
            now = cal.current_time()
            event = ClientToTellerEvent(now, teller, client)
            cal.put(event)
        else:
            self.__free_tellers.put(teller)

    @abstractmethod
    def enqueue(self, client):
        pass

    @abstractmethod
    def has_waiting_client(self):
        pass

    @abstractmethod
    def get_next_client(self):
        pass

class SingleQueue(QueueingSystem):
    """
    A client queueing system that puts all waiting clients in the same queue.
    """

    def __init__(self, calendar):
        QueueingSystem.__init__(self, calendar)
        self.__queue = Queue()

    def enqueue(self, client):
        self.__queue.put(client)

    def has_waiting_client(self):
        return not self.__queue.empty()

    def get_next_client(self):
        return self.__queue.get()

class RandomQueue(QueueingSystem):
    """
    A client queueing system that chooses randomly from waiting clients.
    """

    def __init__(self, calendar):
        QueueingSystem.__init__(self, calendar)
        self.__waiting_clients = []

    def enqueue(self, client):
        self.__waiting_clients.append(client)

    def has_waiting_client(self):
        return len(self.__waiting_clients)

    def get_next_client(self):
        i = random.randrange(len(self.__waiting_clients))
        client = self.__waiting_clients[i]
        self.__waiting_clients.pop(i)
        return client


class Teller(Server):
    """
    Tellers know how to attend a client.
    """

    def __init__(self, calendar, queue, work_rate):
        Server.__init__(self, calendar)
        self.__queue = queue
        self.__work_rate = work_rate
        self.__free_time = []
        self.__last_attending = 0.0

    def attend_client(self, client):
        """Do the work required by the client (takes time). Afterwards, notify queue about free status."""
        curr_time = self.calendar().current_time()
        self.__free_time.append(curr_time - self.__last_attending)
        time_to_finish = client.work() / self.__work_rate
        finish_time = curr_time + time_to_finish
        client.departure(finish_time)
        event = TellerFreeEvent(finish_time, self.__queue, self)
        self.calendar().put(event)
        self.__last_attending = finish_time
        
    def free_times(self):
        return self.__free_time


# Event types

class ClientArrivalEvent(Event):
    """
    A client has arrived.
    """

    def __init__(self, time, queue, client):
        Event.__init__(self, time, queue)
        self.__client = client

    def process(self):
        """Record arrival time in the client and insert it in the queue."""
        self.__client.arrival(self.time())
        self.server().new_client(self.__client)


class ClientToTellerEvent(Event):
    """
    Client goes to a free teller.
    """

    def __init__(self, time, teller, client):
        Event.__init__(self, time, teller)
        self.__client = client

    def process(self):
        """Teller should attend to client."""
        self.server().attend_client(self.__client)


class TellerFreeEvent(Event):
    """
    A teller has become free.
    """

    def __init__(self, time, queue, teller):
        Event.__init__(self, time, queue)
        self.__free_teller = teller

    def process(self):
        """Notify queueing system of the free teller."""
        self.server().free_teller(self.__free_teller)


# Simulations

def simple_simulation(num_clients, work, arrival_time, num_tellers, rate, queueing_system, filename):
    calendar = Calendar()
    queue = queueing_system(calendar)

    tellers = [Teller(calendar, queue, rate(i)) for i in range(num_tellers)]
    for teller in tellers:
        calendar.put(TellerFreeEvent(0.0, queue, teller))

    clients = [Client(work(i)) for i in range(num_clients)]
    for i, client in enumerate(clients):
        calendar.put(ClientArrivalEvent(arrival_time(i), queue, client))

    calendar.process_all_events()

    with open(filename + '_wait.dat', 'w') as outfile:
        for client in clients:
            arrival, departure = client.report()
            print(arrival, departure, file=outfile)

    with open(filename + '_free.dat', 'w') as outfile:
        for teller in tellers:
            freetimes = teller.free_times()
            for t in freetimes:
                print(t, file=outfile)

    return calendar.current_time()


# Code to run

if __name__ == '__main__':
    #t = simple_simulation(100, lambda i: 2, lambda i: 3.0 * i, 1, lambda i: 1, SingleQueue, 'single_oneteller_nooverlap.dat')
    #print('Total time for single queue, one teller, no overlap is', t)
    #t = simple_simulation(100, lambda i: 2, lambda i: 1.0 * i, 1, lambda i: 1, SingleQueue, 'single_oneteller_overlap.dat')
    #print('Total time for single queue, one teller, overlap is', t)
    #t = simple_simulation(100, lambda i: 2, lambda i: 3.0 * i, 1, lambda i: 1, RandomQueue, 'random_oneteller_nooverlap.dat')
    #print('Total time for random queue, one teller, no overlap is', t)
    #t = simple_simulation(100, lambda i: 2, lambda i: 1.0 * i, 1, lambda i: 1, RandomQueue, 'random_oneteller_overlap.dat')
    #print('Total time for random queue, one teller, overlap is', t)
    t = simple_simulation(2500, lambda i: 2 + random.random(), lambda i: random.uniform(0, 2500), 3, lambda i: 1, SingleQueue, 'single_randomclient_twotellers')
    print('Total time for single queue, two tellers, random clients', t)
