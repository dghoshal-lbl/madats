"""
`madats.core.task`
====================================

.. currentmodule:: madats.core.task

:platform: Unix, Mac
:synopsis: Module that defines a workflow task abstraction

.. moduleauthor:: Devarshi Ghoshal <dghoshal@lbl.gov>

"""

import os
import sys
import hashlib
import uuid
from madats.utils.constants import TaskType, Persistence, Policy, UNKNOWN
from madats.core.scheduler import Scheduler

##########################################################################
class Task(object):
    """
    A workflow task object that corresponds to a single stage/step/task/job in the workflow
    """

    def __init__(self, command, type=TaskType.COMPUTE):
        self.__id__  = str(uuid.uuid4())
        self._name = ''
        self._command = command
        self._inputs = []
        self._outputs = []
        self._params = []
        self._expected_runtime = UNKNOWN
        self._priority = UNKNOWN
        self.predecessors = []
        self.successors = []
        self._bin = 0
        self._type = type
        self._scheduler = Scheduler.NONE
        self._scheduler_opts = {}
        self._prerun = []
        self._postrun = []

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def command(self):
        return self._command

    @command.setter
    def command(self, command):
        self._command = command

    @property
    def inputs(self):
        return self._inputs

    @inputs.setter
    def inputs(self, inputs):
        self._inputs = inputs

    @property
    def outputs(self):
        return self._outputs

    @outputs.setter
    def outputs(self, outputs):
        self._outputs = outputs

    @property
    def params(self):
        return self._params

    @params.setter
    def params(self, params):
        self._params = []
        for param in params:
            self.add_param(param)

    def add_param(self, param):
        self._params.append(param)

    @property
    def runtime(self):
        return self._expected_runtime

    @runtime.setter
    def runtime(self, runtime):
        self._expected_runtime = runtime

    @property
    def type(self):
        return self._type

    @property
    def scheduler(self):
        return self._scheduler

    @scheduler.setter
    def scheduler(self, scheduler):
        self._scheduler = scheduler

    @property
    def bin(self):
        return self._bin

    @bin.setter
    def bin(self, bin):
        self._bin = bin

    @property
    def prerun(self):
        return self._prerun

    @prerun.setter
    def prerun(self, prerun):
        self._prerun = prerun

    @property
    def postrun(self):
        return self._postrun

    @postrun.setter
    def postrun(self, postrun):
        self._postrun = postrun

    def add_predecessor(self, t):
        self.predecessors.append(t)

    def add_successor(self, t):
        self.successors.append(t)
    
    @property
    def scheduler_opts(self):
        return self._scheduler_opts

    @scheduler_opts.setter
    def scheduler_opts(self, sched_opts):
        for k, v in sched_opts.items():
            self._scheduler_opts[k] = v

    def get_schedopt(self, opt):
        return self._scheduler_opts[opt]

##########################################################################
class DataTask(Task):
    """
    A datatask that is specifically marked to move data objects between storage tiers
    """

    def __init__(self, vdo_src, vdo_dest):
        Task.__init__(self, command='', type=TaskType.DATA)        
        self.vdo_src = vdo_src
        self.vdo_dest = vdo_dest
        self.add_param(vdo_src.abspath)
        self.add_param(vdo_dest.abspath)
        self.command = self.__set_data_mover__(vdo_src, vdo_dest)

    def __set_data_mover__(self, vdo_src, vdo_dest):
        # the default data mover is 'cp', however, it
        # changes based on the source and destination
        # storage/file systems
        command = 'cp'

        # TODO: add flexible data mover; can take it as an arg as well from the calling function

        return command
        

        
