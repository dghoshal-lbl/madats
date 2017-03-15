'''
Workflow abstraction: interface to translate any workflow into a collection of VDOs
'''

import abc
from plugins import plugin_loader
from core.vds import VirtualDataObject

class WorkflowAbstract():
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        pass


    @abc.abstractmethod
    def parse(self, workflow, type=None, **kwargs):
        #print('Transform a workflow to a collection of VDOs: data as nodes, processes as edges')
        raise NotImplementedError()
