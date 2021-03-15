"""
Implementation of basic classes for programming discrete event simulations.
"""

import queue
from abc import ABCMeta, abstractmethod

class Event(metaclass=ABCMeta):

    """
    Base class of events. An event that occurs at a given time (a float value) and
    must be processed by a given server.
    """

    def __init__(self, time, server):
        """Create a new event for a given server at a given timer."""
        self.__time = time
        self.__server = server

    def time(self):
        """Return event time"""
        return self.__time

    def server(self):
        """Return server associated with the event."""
        return self.__server

    @abstractmethod
    def process(self):
        """Execute the action associated with the event (must be defined in derived classes)."""
        pass

    def __lt__(self, other):
        return self.time() < other.time()


class Server:

    """
    Servers are responsible for the implementation of the methods that will do the work associated with event
    processing. They can generate new events that are inserted in the calendar.
    """

    def __init__(self, calendar):
        self.__cal = calendar

    def calendar(self):
        """Return the calendar associated with this server"""
        return self.__cal


class Calendar:

    """
    Event calendar. The event to be removed is always the one with smallest scheduled time.
    """

    def __init__(self):
        self.__queue = queue.PriorityQueue()
        self.__current_time = 0.0

    def current_time(self):
        """Return the current time (time of last removed event)."""
        return self.__current_time

    def put(self, event):
        """Insert event in the calendar."""
        if not isinstance(event, Event):
            raise TypeError('Argument to Calendar.put must be an Event')
        if event.time() < self.__current_time:
            raise ValueError('New event is previous to last removed event')
        self.__queue.put(event)

    def get(self):
        """Get next event and remove it from calendar."""
        event = self.__queue.get()
        self.__current_time = event.time()
        return event

    def process_all_events(self):
        """Keep processing events until the calendar is empty."""
        while not self.__queue.empty():
            ev = self.get()
            ev.process()

    def process_events_until(self, time):
        """Process events until the next event has timestamp larger than the specified time."""
        while True:
            if self.__queue.empty():
                break
            event = self.__queue.get()
            if event.time() <= time:
                self.__current_time = event.time()
                event.process()
            else:
                self.__queue.put(event)  # Put it back (not yet processed)
                break
