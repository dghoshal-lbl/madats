"""
`madats.utils.constants`
====================================

.. currentmodule:: madats.utils.constants

:platform: Unix, Mac
:synopsis: Module defining constants and types

.. moduleauthor:: Devarshi Ghoshal <dghoshal@lbl.gov>

"""

from madats.utils.config import property_config
import sys

UNKNOWN = -999

class TaskType(object):
    """
    Types of workflow tasks
    """

    COMPUTE = 0
    DATA = 1
    CLEANUP = 2

    name_dict = {COMPUTE: 'Compute', DATA: 'Data', CLEANUP: 'Cleanup'}

    @staticmethod
    def name(type):
        return TaskType.name_dict.get(type, None)


class ExecutionMode(object):
    """
    Workflow execution modes supported in MaDaTS
    """

    DAG = 0
    BIN = 1
    PRIORITY = 2
    DEPENDENCY = 3

    type_dict = {'dag': DAG, 'bin': BIN,
                 'priority': PRIORITY, 'dependency': DEPENDENCY}

    name_dict = {DAG: 'dag', BIN: 'bin',
                 PRIORITY: 'priority', DEPENDENCY: 'dependency'}

    @staticmethod
    def type(name):
        return ExecutionMode.type_dict.get(name, ExecutionMode.DAG)

    @staticmethod
    def name(type):
        return ExecutionMode.name_dict.get(type, None)


class Persistence(object):
    """
    Persistence class assigns different lifetimes to the data
    """

    NONE = 0
    SHORT_TERM = int(property_config.SHORT_TERM)
    LONG_TERM = int(property_config.LONG_TERM)
    FIXED_TERM = int(property_config.FIXED_TERM)

    lifetimes = {'NONE': 0, 'SHORT_TERM': SHORT_TERM,
                 'LONG_TERM': LONG_TERM, 'FIXED_TERM': FIXED_TERM}

    @staticmethod
    def lifetime_in_secs(persistence):
        return Persistence.lifetimes.get(persistence, 0)

    @staticmethod
    def types():
        return Persistence.lifetimes.keys()


class Policy(object):
    """
    Policy class that lists the data management policies currently supported in MaDaTS
    """

    NONE = 0
    WORKFLOW_AWARE = 1
    STORAGE_AWARE = 2

    policy_name = {NONE: 'none', WORKFLOW_AWARE: 'wfa',
                   STORAGE_AWARE: 'sta'}

    policy_type = {'none': NONE, 'wfa': WORKFLOW_AWARE,
                   'sta': STORAGE_AWARE}

    @staticmethod
    def name(policy):
        return Policy.policy_name.get(policy, '')

    @staticmethod
    def type(name):
        return Policy.policy_type.get(name, Policy.NONE)

    @staticmethod
    def policies():
        return Policy.policy_name


"""
test main
"""
if __name__ == '__main__':
    ptypes = Persistence.types()
    for p in ptypes:
        v = Persistence.lifetime_in_secs(p)
        print('{}: {}'.format(p, v))

    print('SHORT_TERM: {}'.format(Persistence.lifetime_in_secs('SHORT_TERM')))


