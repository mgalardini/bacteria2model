#!/usr/bin/python

__author__ = "Marco Galardini"

import os
import threading
try:
    from Queue import Queue
except ImportError:
    from queue import Queue
import time
import multiprocessing
from multiprocessing import queues

import logging

logger = logging.getLogger('bacteria2model.threading')

class Status(object):
    '''
    Class Status
    Gives informations about the run status of a specific thread
    '''
    def __init__(self,status=None,msg=None,maxstatus=None,
                    substatus=None,submsg=None,maxsubstatus=None,
                    fail=False):
        self.status = status
        self.msg = msg
        self.maxstatus = maxstatus
        #
        self.substatus = substatus
        self.submsg = submsg
        self.maxsubstatus = maxsubstatus
        # Fail msg?
        self.fail = fail

class CommonThread(threading.Thread):
    '''
    Class CommonThread: Common operations for a threading class
    '''
    _statusDesc = {0:'Not started',
               1:'Making room', 
               3:'Cleaning up'}
    
    _substatuses = []
    
    def __init__(self,queue=Queue()):
        threading.Thread.__init__(self)
        # Thread
        self.msg = queue
        self._status = 0
        self._maxstatus = len(self._statusDesc)
        self._substatus = 0
        self._maxsubstatus = 0
        self._room = None
        self.killed = False
        
    def getStatus(self):
        return self._statusDesc[self._status]
    
    def getMaxStatus(self):
        return self._maxstatus
    
    def getMaxSubStatus(self):
        return self._maxsubstatus
    
    def getSubStatuses(self):
        return self._substatuses
    
    def resetSubStatus(self):
        self._substatus = 0
        self._maxsubstatus = 0
        
    def makeRoom(self,location=''):
        '''
        Creates a tmp directory in the desired location
        '''
        try:
            path = os.path.abspath(location)
            path = os.path.join(path, 'tmp')
            self._room = path
            os.mkdir(path)
        except:
            logger.debug('Temporary directory creation failed! %s'
                          %path)
    
    def startCleanUp(self):
        '''
        Removes the temporary directory
        '''
        if os.path.exists(self._room):
            logger.debug('Removing the old results directory (%s)'%
                         self._room)
            shutil.rmtree(self._room, True)
    
    def cleanUp(self):
        '''
        Removes the temporary directory
        '''
        shutil.rmtree(self._room, True)
        
    def run(self):
        self.updateStatus()
        self.makeRoom()

        self.updateStatus()
        self.cleanUp()
            
    def sendFailure(self,detail='Error!'):
        msg = Status(fail=True,
                     msg=detail)
        self.msg.put(msg)
        # Give some time for the message to arrive
        time.sleep(0.1)
        
    def updateStatus(self,sub=False,send=True):
        if not sub:
            self._status += 1
        if not send:
            return
        if self._status in self._substatuses:
            msg = Status(status=self._status,msg=self.getStatus(),
                         maxstatus=self.getMaxStatus(),
                         substatus=self._substatus,
                         maxsubstatus=self.getMaxSubStatus())
        else:
            msg = Status(status=self._status,msg=self.getStatus(),
                         maxstatus=self.getMaxStatus())
        self.msg.put(msg)
        
    def kill(self):
        self.killed = True

class SafeSleep(object):
    '''
    IOError safe sleep
    '''
    def sleep(self,seconds):
        '''
        Sleeps for a certain amount of seconds
        Raises an exception if too many errors are encountered
        '''
        dt = 1e-3
        while dt < 1:
            try:
                time.sleep(seconds)
                return
            except IOError:
                logger.warning('IOError encountered in SafeSleep sleep()')
                try:
                    time.sleep(dt)
                except:pass
                dt *= 2
                
        e = IOError('Unrecoverable error')
        raise e

class SafeQueue(queues.Queue):
    '''
    IOError safe multiprocessing Queue
    '''
    def __init__(self):
        queues.Queue.__init__(self)
        
    def empty(self):
        '''
        Returns True if the Queue is empty, False otherwise
        Raises an exception if too many errors are encountered 
        '''
        dt = 1e-3
        while dt < 1:
            try:
                isEmpty = queues.Queue.empty(self)
                return isEmpty
            except IOError:
                logger.warning('IOError encountered in SafeQueue empty()')
                try:
                    time.sleep(dt)
                except:pass
                dt *= 2
                
        e = IOError('Unrecoverable error')
        raise e

    def get(self):
        '''
        Get the element in the queue
        Raises an exception if it's empty or if too many errors are
        encountered
        '''
        dt = 1e-3
        while dt < 1:
            try:
                element = queues.Queue.get(self)
                return element
            except IOError:
                logger.warning('IOError encountered in SafeQueue get()')
                try:
                    time.sleep(dt)
                except:pass
                dt *= 2
                
        e = IOError('Unrecoverable error')
        raise e
    
    def put(self,element):
        '''
        Put the element in the queue
        Raises an exception if too many errors are
        encountered
        '''
        dt = 1e-3
        while dt < 1:
            try:
                queues.Queue.put(self,element)
                return
            except IOError:
                logger.warning('IOError encountered in SafeQueue put()')
                try:
                    time.sleep(dt)
                except:pass
                dt *= 2
                
        e = IOError('Unrecoverable error')
        raise e

class Consumer(multiprocessing.Process):    
    def __init__(self, 
                 task_queue = multiprocessing.Queue(),
                 result_queue = multiprocessing.Queue()):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.sleeper = SafeSleep()

    def run(self):
        while True:
            next_task = self.task_queue.get()
            self.sleeper.sleep(0.01)
            if next_task is None:
                # Poison pill means we should exit
                break
            answer = next_task()
            self.result_queue.put(answer)
        return

class CommonMultiProcess(CommonThread):
    '''
    Class CommonMultiProcess
    A Thread that can perform multiprocesses
    '''
    def __init__(self,ncpus=1, queue=queues.Queue()):
        CommonThread.__init__(self,queue)
        
        self.ncpus = int(ncpus)
        # Parallelization
        self._parallel = None
        self._paralleltasks = SafeQueue()
        self._parallelresults = SafeQueue()
        self.sleeper = SafeSleep()
        
        # ID
        self._unique = 0
        
    def getUniqueID(self):
        self._unique += 1
        return self._unique
    
    def initiateParallel(self):
        self._parallel = [Consumer(self._paralleltasks,self._parallelresults)
                          for x in range(self.ncpus)]
        for consumer in self._parallel:
            consumer.start()
            
    def addPoison(self):
        for consumer in self._parallel:
            self._paralleltasks.put(None)

    def isTerminated(self):
        for consumer in self._parallel:
            if consumer.is_alive():
                return False
        return True

    def killParallel(self):
        for consumer in self._parallel:
            consumer.terminate()
