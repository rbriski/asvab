#!/usr/local/bin/python

import networkx
from networkx import DiGraph, Graph
from job import Job
import time

g = DiGraph()
start = Job('start', 'nothin')
start.done = True
start.succeeded = True

test1 = Job('tst', 'bob.py')
test2 = Job('tst2', 'test1.py')
test3 = Job('tst3', 'bob.py')

g.add_edge(start,test1)
g.add_edge(start,test2)
g.add_edge(test1,test3)
g.add_edge(test2,test3)

def walk(job):
    if job.isDone():
        print "%s is done." % job.jobId
        done = True
        for child in g.successors(job):
            print "Checking %s..." % child.jobId
            retVal = walk(child)
            print "%s branch dead: %s" % (job.jobId, str(retVal))
            done = done & retVal
        return done

    if job.isRunning():
        return False
    
    for parent in g.predecessors(job):
        #If the parent isn't done, we can't continue but the
        #branch is not yet dead
        print "Check if the parent is done..."
        if not parent.isDone():
            return False

        #If this job needs success from its parents and doesn't get
        #it, the branch is dead, return True (for branch finished)
        print "Checking if it needsSuccess..."
        if job.needsSuccess and (not parent.succeeded):
            return True

    #If it passes the above gauntlet, let's start the process
    #and pass a False because the branch is not dead
    print "running %s..." % job.jobId
    job.run()
    return False

done = False
while not done:
    done = walk(start)
    print " ***************************** "
    time.sleep(2)
