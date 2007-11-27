#!/usr/local/bin/python

from subprocess import Popen
from datetime import datetime
import time
import os
import pprint

class Job(object):
    """ Single process job

    Represents a single job, currently a script file to be run.  Encapsulates
    this process and gets return value, stdout and sterr and writes these
    values to a file.  Also keeps stats of the job.

    Stats include:
      script location
      start time
      end time
      return value
      whether the job is running
      if the job succeeded
      if the job is done
    """
    root = os.getcwd()
    statKeys = ['jobId', 'startTime', 'endTime', 'retCode']

    def __init__(self, jobId, script, logDir=None):
        self.jobId = jobId
        self.logDir = logDir

        #Script file location
        self.script = os.path.join(self.root, script)

        #If this script needs its parent to succeed
        #TODO: Move this out into the workflow manager
        #the job should not need to know anything outside
        #of itself
        self.needsSuccess = True

        #Out, error, and stat files
        self.out = None
        self.err = None
        self.stat = None

        #Stats
        self.startTime = None
        self.pid = None
        self.endTime = None
        self.retCode = None

        #The actual running process
        self.job = None

        #Terminating information
        self.done = False
        self.running = False
        self.succeeded = False
        self.starved = False

    def getFile(self, ts, extension):
        """ Opens a file based on the timestamp and extension
        """
        fileLoc = self.getFileName(ts, extension)
        return open(fileLoc, 'w')

    def getFileName(self, ts, extension):
        """ Gets a filename based on a timestamp and extension
        """
        fileName = '.'.join([str(self.jobId), str(ts), extension])
        if self.logDir is None:
            self.logDir = self.root
        fileLoc = os.path.join(self.logDir, fileName)
        return fileLoc
    
    def run(self):
        """Runs the process

        Opens all output files
        Timestamps the start time
        Runs the script
        """
        startTS = int(time.time())
        self.out = self.getFile(startTS, 'out')
        self.err = self.getFile(startTS, 'err')
        self.stat = self.getFile(startTS, 'stat')

        self.startTime = datetime.fromtimestamp(startTS)

        print self.script
        self.job = Popen(self.script, stdout=self.out, stderr=self.err)
        self.pid = self.job.pid
        self.running = True

    def hasStarved(self):
        """An upstream job has failed and
        this one cannot be reached
        """

        self.done=True
        self.running=False
        self.starved=True

    def isRunning(self):
        """Is the job running?
        """
        return self.running

    def isDone(self):
        """Is the job done?

        Polls the job
        Grabs return code if its finished
        Timestamps the endtime
        Prints the stat file
        Closes all file handles
        """
        #If it's done, it's done
        if self.done is True:
            return self.done

        print '-- Passed the done check'
        #If we're not running and not done,
        #it hasn't even started
        if self.isRunning() is False:
            return False

        print ' -- Passed the isRunning check'
        #Poll the job and grab the ret code
        self.job.poll()
        if self.job.returncode is None:
            return False

        print ' -- Passed the returncode check'
        #We're now done, set the status bits
        self.done = True
        self.running = False
        self.retCode = self.job.returncode

        #If ret code is 0, we didn't exit
        #with an error
        if self.retCode == 0:
            self.succeeded = True

        #Print out our stats
        endTS = int(time.time())
        self.endTime = datetime.fromtimestamp(endTS)
        pp = pprint.PrettyPrinter(stream=self.stat)
        statInfo = dict((key, getattr(self, key)) for key in self.statKeys)
        pp.pprint(statInfo)

        #Close the filehandles
        self.out.close()
        self.err.close()
        self.stat.close()

        return self.done

    

# j = Job('tst')
# j1 = Job('tst1')
# print 'instantiated'


# j.run('bob.py')
# j1.run('test1.py')
# print 'running'


# while not (j.isDone() and j1.isDone()):
#     if j.isDone():
#         print 'tst done'
#     if j1.isDone():
#         print 'tst1 done'
#     print 'cycle'
#     time.sleep(1)

