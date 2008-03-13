#!/usr/local/bin/python

import networkx
from distutils.dir_util import mkpath
from networkx import DiGraph, Graph
from asvab.job import Job
from asvab.config import Config
import time, os, sys
import yaml
import pprint
import commands

class WorkFlow(object):
    """A collection of directed jobs

    Represents a groups of jobs and their dependencies.  Can run the jobs according
    to a configuration file and collect their output.  The jobs are stored in a
    directed acyclical graph (DAG).

    @cvar root: The directory this file is in
    @type root: C{string}
    @cvar configRoot: root + 'conf'
    @type configRoot: C{string}
    @cvar logRoot: root + 'log'
    @type logRoot: C{string}
    @ivar name: Name of this workflow
    @type name: C{string}
    @ivar confPath: Path to the configuration file for this flow
    @type confPath: C{string}
    @ivar flowLogRoot: Path to the log dir for this flow
    @type flowLogRoot: C{string}
    @ivar jobLogRoot: Path to the log dir for all jobs of this flow
    @type jobLogRoot: C{string}
    @ivar conf: Complete configuration for this flow
    @type conf: C{dict}
    @ivar email: Email for all messages
    @type email: C{string}
    @ivar iterPause: Length of pause (in seconds) between walks of the DAG
    @type iterPause: C{int}    
    """

    #The current directory of this file
    root = sys.path[0]

    #The default dir for config files
    configRoot = os.path.join(root, 'conf')

    #The default dir for log files
    logRoot = os.path.join(root, 'log')

    def __init__(self, name='default', iterPause=5):
        """
        @keyword name: The name of this workflow.  Also determines the
        name of the conf file to be used and the log directory.  The default
        value is "default"
        @type name: C{string}
        @keyword iterPause: The length of time (in seconds) that the workflow
        will pause before walking the job graph again, The default is 5 seconds.
        @type iterPause: C{int}

        """
        #Conf files, log directories are named after the workflow
        self.name = name
        self.confPath = os.path.join(self.configRoot, self.name)
        self.confPath += '.yml'

        #Start is the starting node of the DAG, it's essentially an
        #empty node but must be present to walk the graph correctly
        self.start = None

        #Set up my logging root
        currTime = int(time.time())
        self.flowLogRoot = os.path.join(self.logRoot, self.name)
        self.jobLogRoot = os.path.join(self.flowLogRoot, str(currTime))
        mkpath(self.jobLogRoot)

        self.pid = os.getpid()
        self.pidFile = os.path.join(self.flowLogRoot, "%s.pid" % self.name)

        #Some configuration details
        self._loadFlow = Config(self.confPath)
        self.conf = self._loadFlow()
        self.email = self.conf.get('email')
        self.iterPause = iterPause

        #Set up the DAG
        self.graph = self._buildGraph(self.conf['jobs'])

        #Create the "latest" symlink
        latestLink = os.path.join(self.flowLogRoot, 'latest')
        if os.path.exists(latestLink):
            os.remove(latestLink)
        os.symlink(self.jobLogRoot, latestLink)


    def _buildGraph(self, jobs):
        """ Builds the workflow graph

        Uses the dependencies specified in the configuration
        file to build a directed acyclic graph.  This graph
        is used to direct which process are run and when.

        @param jobs: A list of job attributes and dependencies
        from the conf file
        @type jobs: C{list}
        """
        h = {}

        #Set up my graph and the dummy start noe
        g = DiGraph()
        self.start = Job('start', 'nothin', logDir=self.jobLogRoot)
        self.start.done = True
        self.start.succeeded = True

        #Make a dict with the job id as the key
        #Put the instantiated jobs and dependencies
        #in as values
        for node in list(jobs):
            id = node['id']
            del node['id']
            h[id] = {}

            if 'homedir' not in node:
                node['homedir'] = WorkFlow.root
            env = os.environ
            if 'env' in node:
                env.update(node['env'])
                
            job = Job(id, node['script'], homeDir=node['homedir'], logDir=self.jobLogRoot, env=env)
            h[id]['job'] = job
            if 'depends_on' in node:

                #Quack: depends_on must be a list, may be passed
                #as a string.  This bit of duck typing makes sure
                #it's a list
                if not hasattr(node['depends_on'], 'sort'):
                    node['depends_on'] = [node['depends_on']]

                h[id]['depends_on'] = node['depends_on']

        #Use the dict to build the DAG
        for jobId, v in h.items():
            if 'depends_on' in v:
                for j in v['depends_on']:
                    g.add_edge(h[j]['job'], v['job'])
            else:
                g.add_edge(self.start, v['job'])

        return g

    def run(self):
        """ Run the workflow

        Will walk the graph every so often until
        each node has been run or has been declared
        dead

        A branch is dead when a parent node fatally exits
        """
        
#         if os.path.exists(self.pidFile):
#             oldPid = file(self.pidFile).read()
#             sPids = commands.getoutput('ps -eo pid')
#             pids = sPids.split('\n')
#             print pids
#             if oldPid in pids:
#                 print "Job is currently running"
#                 exit(0)

#         pidFile = open(self.pidFile, 'w')
#         pidFile.write(str(self.pid))
#         pidFile.close()

        done = False
        while not done:
            done = self._walk(self.start)
            time.sleep(self.iterPause)
        
    def _walk(self, job):
        """ Walk the DAG

        Walks the directed graph from root to leaves.  It can
        only advance to a lower node once the parent has finished.  The child
        checks if both parents are finished before running itself.  If
        a node dies fatally, the rest of that branch is declared starved.
        """
        # If the current job is done we'll set our local "done" var to true
        # and then recurse through the child nodes.  Their return values
        # are anded with this one to see if all nodes have been walked.
        # If so, we're finished and return True to the run method.
        if job.isDone():
            done = True
            for child in self.graph.successors(job):
                retVal = self._walk(child)
                done = done & retVal
            return done

        #If the job is still running, it's not done
        if job.isRunning():
            return False
    
        for parent in self.graph.predecessors(job):

            #If the parent isn't done, we can't continue but the
            #branch is not yet dead
            if not parent.isDone():
                return False

            #If this job needs success from its parents and doesn't get
            #it, the branch is dead, return True (for branch finished)
            if job.needsSuccess and (not parent.succeeded):
                self.kill(job) #Kill all children, they've starved
                return True

        #If it passes the above gauntlet, let's start the process
        #and pass a False because the branch is not dead
        job.run()
        return False

    def kill(self, job):
        """ Kills the current job as well as all children

        @param job: Job to kill
        @type job: L{Job}
        """
        job.hasStarved()
        for j in self.graph.successors(job):
            self.kill(j)


