#!/usr/local/bin/python

from subprocess import Popen
from datetime import datetime
import time
import os
import pprint
import yaml

class Job(object):
    """ Single process job

    Represents a single job, currently a script file to be run.  Encapsulates
    this process and gets return value, stdout and sterr and writes these
    values to a file.  Also keeps stats of the job.

    Stats include:
      - script location
      - start time
      - end time
      - return value
      - whether the job is running
      - if the job succeeded
      - if the job is done

    @cvar statKeys: variables that will have their values dumped into
    a stats table upon completion of the job
    @type statKeys: C{list}
    @ivar jobId: the id of the current job (not a process id)
    @type jobId: C{string}
    @ivar logDir: the directory where the log files from this job
    are dumped
    @type logDir: C{string}
    @ivar homeDir: the home directory of this job, used as CWD in the environment
    @type homeDir: C{string}
    @ivar script: Actual command to run the job
    @type script: C{string} or C{list}
    @ivar needsSuccess: Whether this job needs its parent to successfully execute to run
    @type needsSuccess: C{boolean}
    @ivar env: Environment of the job
    @type env: C{dict}
    @ivar done: If the job is done
    @type done: C{boolean}
    @ivar running: If the job is running
    @type running: C{boolean}
    @ivar succeeded: If the job succeeded
    @type succeeded: C{boolean}
    @ivar starved: If the jobs parents failed
    @type starved: C{boolean}
    
    """
    statKeys = ['jobId', 'startTime', 'endTime', 'retCode', 'starved', 'succeeded']

    def __init__(self, jobId, script, logDir=None, homeDir=None, env=None):
        """
        Initializes but DOES NOT run the job

        @param jobId: The id or name of the job
        @type jobId: C{string}
        @param script: The actual command to run
        @type script: C{string} or C{list}
        @keyword logDir: The directory where log files will be created
        @type logDir; C{string}
        @keyword homeDir: The CWD of this job
        @type homeDir: C{string}
        @keyword env: The environment of this job
        @type env: C{dict}
        """
        self.jobId = jobId
        
        if logDir is None:
            logDir=os.getcwd()
        self.logDir = logDir

        self.homeDir = homeDir

        #Script file location
        self.script = script.split()

        #If this script needs its parent to succeed
        #TODO: Move this out into the workflow manager
        #the job should not need to know anything outside
        #of itself
        self.needsSuccess = True

        #Out, error, and stat files
        self._out = self._getFile('out')
        self._err = self._getFile('err')
        self._stat = self._getFile('stat')

        #Stats
        self.startTime = None
        self.pid = None
        self.endTime = None
        self.retCode = None

        self.env = env

        #The actual running process
        self.job = None

        #Terminating information
        self.done = False
        self.running = False
        self.succeeded = False
        self.starved = False

    def __del__(self):
        """ Deconstructor
        """
        #Close the filehandles
        self._out.close()
        self._err.close()
        self._stat.close()



    def _getFile(self, extension):
        """ Opens a file based on the timestamp and extension
        """
        fileLoc = self._getFileName(extension)
        return open(fileLoc, 'w')

    def _getFileName(self, extension):
        """ Gets a filename based on a timestamp and extension
        """
        fileName = '.'.join([str(self.jobId), extension])
        fileLoc = os.path.join(self.logDir, fileName)
        return fileLoc
    
    def run(self):
        """Runs the process

        Opens all output files
        Timestamps the start time
        Runs the script
        """
        startTS = int(time.time())
        self.startTime = datetime.fromtimestamp(startTS)

        self.job = Popen(self.script, stdout=self._out, stderr=self._err, cwd=self.homeDir, env=self.env)
        self.pid = self.job.pid
        self.running = True

    def hasStarved(self):
        """Tells this job not to run because an upstream
        job has failed
        """

        self.done=True
        self.running=False
        self.starved=True
        self.printStats()

    def isRunning(self):
        """Is the job running?
        """
        return self.running

    def printStats(self):
        """Prints the stats of this job to its stats file
        """
        statInfo = dict((key, getattr(self, key)) for key in self.statKeys)
        yaml.dump(statInfo, stream=self._stat)
        
    def isDone(self):
        """Is the job done?

        Steps:
          - Polls the job
          - Grabs return code if its finished
          - Timestamps the endtime
          - Prints the stat file
          - Closes all file handles
        """
        #If it's done, it's done
        if self.done is True:
            return self.done

        #If we're not running and not done,
        #it hasn't even started
        if self.isRunning() is False:
            return False

        #Poll the job and grab the ret code
        self.job.poll()
        if self.job.returncode is None:
            return False

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
        self.printStats()

        return self.done
