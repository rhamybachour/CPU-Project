"""
Run some simulations of an Operating System.
"""

from desimul import Calendar, Event, Server
from queue import Queue
from abc import ABCMeta, abstractmethod
import random

#########################################################################################################################
################################################ Auxiliary classes ######################################################
#########################################################################################################################
class Task:
    """
    Implements methods to verify waiting times for each task.
    """

    def __init__(self, work):
        self.__arrival_time = None
        self.__departure_time = None
        self.__total_work = work
        self.__processor = None

    def arrival(self, time):
        """Called when task arrives."""
        self.__arrival_time = time

    def departure(self, time):
        """Called when task departs."""
        self.__departure_time = time

################################################ Modified Region ########################################################
    def report(self):
        """Report on arrival, departure times and last processor (for statistics)."""
        return self.__arrival_time, self.__departure_time, self.__processor

    def get_work(self):
        """Set the runtime."""
        return self.__total_work
    
    def set_work(self, time):
        """Get the runtime."""
        self.__total_work = time
    
    def set_processor(self, processor):
        """Set the last task processor."""
        self.__processor = processor
        
    def get_processor(self):
        """Get the last task processor."""
        return self.__processor
#########################################################################################################################
    

#########################################################################################################################
################################################ Server classes #########################################################
#########################################################################################################################
class Scheduler(Server,metaclass=ABCMeta):
    """
    Base class for all task queueing systems.
    """
###################### __init__ Modified (scheduling_time,exchange_time,time_process,quantum) ###########################
    def __init__(self, calendar, scheduling_time, exchange_time, quantum):
        Server.__init__(self, calendar)
        self.__free_processors = Queue()
################################################ Modified Region ########################################################
        self.__scheduling_time = scheduling_time
        self.__exchange_time = exchange_time
        self.__quantum = quantum
#########################################################################################################################
    def new_task(self, task):
        """A new task to attend. Either send it to a free processor (if available) or to waiting queue."""
        if self.__free_processors.empty():
            self.enqueue(task)
        else:
            processor = self.__free_processors.get()
            cal = self.calendar()
################################################ Modified Region ########################################################
            now = cal.current_time() +  self.__scheduling_time
            processor.set_time_process(self.__quantum)
            if task.get_processor() is not processor or processor.get_last_attending() == 0.0:
                processor.set_time_process(processor.get_time_process() - self.__exchange_time)
#########################################################################################################################
            event = TaskToProcessorEvent(now, processor, task)
            cal.put(event)

    def free_processor(self, processor):
        """A new free processor. Send it a task (if available) or put in waiting queue."""
        if self.has_waiting_task():
            task = self.get_next_task()
            cal = self.calendar()
################################################ Modified Region ########################################################
            now = cal.current_time() +  self.__scheduling_time
            processor.set_time_process(self.__quantum)
            if task.get_processor() is not processor  or processor.get_last_attending() == 0.0:
                processor.set_time_process(processor.get_time_process() - self.__exchange_time)
#########################################################################################################################
            event = TaskToProcessorEvent(now, processor, task)
            cal.put(event)
        else:
            self.__free_processors.put(processor)

    @abstractmethod
    def enqueue(self, task):
        pass

    @abstractmethod
    def has_waiting_task(self):
        pass

    @abstractmethod
    def get_next_task(self):
        pass

class SingleQueue(Scheduler):
    """
    A task queueing system that puts all waiting tasks in the same queue.
    """
########################## __init__ Modified (scheduling_time,exchange_time,time_process) ###############################
    def __init__(self, calendar, scheduling_time, exchange_time, quantum):
        Scheduler.__init__(self, calendar, scheduling_time, exchange_time, quantum)
        self.__queue = Queue()

    def enqueue(self, task):
        self.__queue.put(task)

    def has_waiting_task(self):
        return not self.__queue.empty()

    def get_next_task(self):
        return self.__queue.get()

class Processor(Server):
    """
    Processors know how to attend a task.
    """
############################################ __init__ Modified (quantum) ################################################
    def __init__(self, calendar, queue, quantum):
        Server.__init__(self, calendar)
        self.__queue = queue
        self.__free_time = []
        self.__last_attending = 0.0
################################################ Modified Region ########################################################
        self.__time_process = quantum
        self.__quantum = quantum
#########################################################################################################################

    def attend_task(self, task):
        """Do the work required by the task (takes time). Afterwards, notify queue about free status."""
        curr_time = self.calendar().current_time()
        self.__free_time.append(curr_time - self.__last_attending)
        print('O',self,'vai processar apenas %.1f' % self.__time_process,'unidade(s) de tempo')
################################################ Modified Region ########################################################        
        time_to_finish = task.get_work()
        print('Falta processar %.1f' % time_to_finish,'unidade(s) de tempo da',task)
        if time_to_finish > self.__quantum:
            finish_time = curr_time + self.__quantum
            task.departure(finish_time)
            event = ProcessorFreeEvent(finish_time, self.__queue, self)
            print('A',task,'parou em %.1f' % finish_time,'unidades de tempo')
            self.calendar().put(event)
            self.__last_attending = finish_time
            task.set_processor(self)
            new_time_to_finish = time_to_finish - self.__time_process
            task.set_work(new_time_to_finish)
            print('Ainda falta processar %.1f' % new_time_to_finish,'unidades de tempo')
            print('\n')
            event = TaskArrivalEvent(finish_time,self.__queue,task)
            self.calendar().put(event)
        else:
            finish_time = curr_time + time_to_finish
            task.departure(finish_time)
            event = ProcessorFreeEvent(finish_time, self.__queue, self)
            print('Finalmente a',task,'terminou em %.1f' % time_to_finish,'unidade(s) de tempo')
            print('\n')
            self.calendar().put(event)
            self.__last_attending = finish_time
#########################################################################################################################

    def free_times(self):
        return self.__free_time
    
    def get_time_process(self):
        return self.__quantum
        
    def set_time_process(self, time_process):
        self.__time_process = time_process
        
    def get_last_attending(self):
        return self.__last_attending
    
#########################################################################################################################
################################################### Event types #########################################################
#########################################################################################################################
class TaskArrivalEvent(Event):
    """
    A task has arrived.
    """

    def __init__(self, time, queue, task):
        Event.__init__(self, time, queue)
        self.__task = task

    def process(self):
        """Record arrival time in the task and insert it in the queue."""
################################################ Modified Region ########################################################
        if self.__task.get_processor() == None:
            self.__task.arrival(self.time())
#########################################################################################################################
        self.server().new_task(self.__task)


class TaskToProcessorEvent(Event):
    """
    Task goes to a free processor.
    """

    def __init__(self, time, processor, task):
        Event.__init__(self, time, processor)
        self.__task = task

    def process(self):
        """Processor should attend to task."""
        self.server().attend_task(self.__task)


class ProcessorFreeEvent(Event):
    """
    A processor has become free.
    """

    def __init__(self, time, queue, processor):
        Event.__init__(self, time, queue)
        self.__free_processor = processor

    def process(self):
        """Notify queueing system of the free processor."""
        self.server().free_processor(self.__free_processor)

#########################################################################################################################
#################################################### Simulations ########################################################
#########################################################################################################################

############################## Modified Header (quantum,scheduling_time,exchange_time)###################################
def simple_simulation(num_tasks, work, quantum, rate, num_processors, scheduling_time, exchange_time, queueing_system, filename):
    calendar = Calendar()
    queue = queueing_system(calendar,scheduling_time,exchange_time,quantum)
################################################ Modified Region ########################################################
    processors = [Processor(calendar, queue, quantum) for i in range(num_processors)]
#########################################################################################################################    
    for processor in processors:
        calendar.put(ProcessorFreeEvent(0.0, queue, processor))

    tasks = [Task(work(i)) for i in range(num_tasks)]
    for i, task in enumerate(tasks):
        if i == 0:
            time = 0.0
        else:
            time += random.expovariate(rate)
        calendar.put(TaskArrivalEvent(time, queue, task))
    
    calendar.process_all_events()

    with open(filename + '_wait.dat', 'w') as outfile:
        for task in tasks:
            arrival, departure, processor = task.report()
            print('%.1f' % arrival, '%.1f' % departure, processor, file=outfile)

    with open(filename + '_free.dat', 'w') as outfile:
        for processor in processors:
            sum_t = 0
            freetimes = processor.free_times()
            for t in freetimes:
                sum_t += t
            print('O',processor,'ficou %.1f' % sum_t,'unidade(s) de tempo livre', file=outfile)
    return calendar.current_time()

def basic_simulation(num_tasks, work, quantum, arrival_time, num_processors, scheduling_time, exchange_time, queueing_system, filename):
    calendar = Calendar()
    queue = queueing_system(calendar,scheduling_time,exchange_time,quantum)
################################################ Modified Region ########################################################
    processors = [Processor(calendar, queue, quantum) for i in range(num_processors)]
#########################################################################################################################    
    for processor in processors:
        calendar.put(ProcessorFreeEvent(0.0, queue, processor))

    tasks = [Task(work(i)) for i in range(num_tasks)]
    for i, task in enumerate(tasks):
        calendar.put(TaskArrivalEvent(arrival_time(i), queue, task))
    
    calendar.process_all_events()

    with open(filename + '_wait.dat', 'w') as outfile:
        for task in tasks:
            arrival, departure, processor = task.report()
            print('%.1f' % arrival, '%.1f' % departure, processor, file=outfile)

    with open(filename + '_free.dat', 'w') as outfile:
        for processor in processors:
            sum_t = 0
            freetimes = processor.free_times()
            for t in freetimes:
                sum_t += t
            print('O',processor,'ficou %.1f' % sum_t,'unidade(s) de tempo livre', file=outfile)
    return calendar.current_time()

#########################################################################################################################
#################################################### Code to run ########################################################
#########################################################################################################################
if __name__ == '__main__':
######################################################## Run! ###########################################################
    #t = simple_simulation(3, lambda i: random.lognormvariate(2, 1.0), 1, 0.25, 2, 0.1, 0.3, SingleQueue, 'simulation.dat')
    #print('Total time for single queue, one processor, no overlap is', t)
#########################################################################################################################

######################################################## Test! ##########################################################
    t = basic_simulation(3, lambda i: 5, 1, lambda i: 2.0*i, 2, 0.1, 0.3, SingleQueue, 'test.dat')
    print('Total time for single queue, two processors, random tasks %.1f' % t)
    
#########################################################################################################################
