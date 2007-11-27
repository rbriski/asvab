#!/usr/local/bin/python

import networkx
from networkx import DiGraph, Graph
from job import Job
import time, os, sys
import yaml
import pprint

class JobServer(object):

    root = os.getcwd()
    configRoot = os.path.join(root, 'conf')
    logRoot = os.path.join(root, 'log')

    def __init__(self, name='default'):
        self.name = name
        self.confPath = os.path.join(self.configRoot, self.name)
        self.confPath += '.yml'
        self.start = None

        self.conf = self.loadFlow(self.confPath)
        self.email = self.conf['email']
        self.graph = self.buildGraph(self.conf['jobs'])
        
    def loadFlow(self, path):
        try:            
            fYaml = open(path, 'r')
            conf = yaml.load(fYaml)
            fYaml.close()
            return conf
        except yaml.scanner.ScannerError:
            print " ** ERROR ** "
            print "Conf file %s is screwed." % path

    def buildGraph(self, jobs):
        h = {}

        g = DiGraph()
        self.start = Job('start', 'nothin', logDir=self.logRoot)
        self.start.done = True
        self.start.succeeded = True
        
        for node in list(jobs):
            id = node['id']
            del node['id']
            h[id] = {}

            job = Job(id, node['script'], self.logRoot)
            h[id]['job'] = job
            if 'depends_on' in node:

                #Quack: depends_on must be a list, may be passed
                #as a string.  This bit of duck typing makes sure
                #it's a list
                if not hasattr(node['depends_on'], 'sort'):
                    node['depends_on'] = [node['depends_on']]

                h[id]['depends_on'] = node['depends_on']

        for jobId, v in h.items():
            if 'depends_on' in v:
                for j in v['depends_on']:
                    g.add_edge(h[j]['job'], v['job'])
            else:
                g.add_edge(self.start, v['job'])
            
        return g

    def run(self):
        done = False
        while not done:
            done = self.walk(self.start)
            print " ***************************** "
            time.sleep(2)
        
    def walk(self, job):
        print 'Walking %s' % job.jobId
        if job.isDone():
            print "%s is done." % job.jobId
            done = True
            for child in self.graph.successors(job):
                retVal = self.walk(child)
                done = done & retVal
            return done

        print '%s running: %s' % (job.jobId, str(job.running))
        print '%s done: %s' % (job.jobId, str(job.done))
        print '%s succeeded: %s' % (job.jobId, str(job.succeeded))
        print '%s starved: %s' % (job.jobId, str(job.starved))
        if job.isRunning():
            print "%s is running..." % job.jobId
            return False
    
        for parent in self.graph.predecessors(job):

            #If the parent isn't done, we can't continue but the
            #branch is not yet dead
            if not parent.isDone():
                print '%s parent is not done' % job.jobId
                return False

            #If this job needs success from its parents and doesn't get
            #it, the branch is dead, return True (for branch finished)
            if job.needsSuccess and (not parent.succeeded):
                self.kill(job)
                return True

        #If it passes the above gauntlet, let's start the process
        #and pass a False because the branch is not dead
        print "Starting %s..." % job.jobId
        job.run()
        return False

    def kill(self, job):
#        if job.isDone():
#            return
        
        print "Killing %s " % job.jobId
        job.hasStarved()
        for j in self.graph.successors(job):
            self.kill(j)

    

js = JobServer()
js.run()
