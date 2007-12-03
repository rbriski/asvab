
from mako.template import Template
import yaml

class Config(object):

    def __init__(self, path):
        self.path = path

    def __call__(self):
        try:            
            tConf = Template(filename=self.path)
            strConf = tConf.render()        
            yConf = yaml.load(strConf)
            return yConf
        except yaml.scanner.ScannerError:
            print ' ** ERROR ** '
            print 'Conf file %s is screwed.' % self.path
