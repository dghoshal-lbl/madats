"""
`madats.utils.constants`
====================================

.. currentmodule:: madats.utils.constants

:platform: Unix, Mac
:synopsis: Module defining constants and types

.. moduleauthor:: Devarshi Ghoshal <dghoshal@lbl.gov>

"""

from madats.utils.config import property_config, slurm_config, pbs_config
import sys

UNKNOWN = -999

class TaskType(object):
    """
    Types of workflow tasks
    """

    COMPUTE = 0
    DATA = 1


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

    policy_name = {NONE: 'NONE', WORKFLOW_AWARE: 'WORKFLOW_AWARE',
                   STORAGE_AWARE: 'STORAGE_AWARE', USER_DRIVEN: 'USER_DRIVEN'}

    @staticmethod
    def name(policy):
        return Policy.policy_name.get(policy, '')


"""
test main
"""
if __name__ == '__main__':
    ptypes = Persistence.types()
    for p in ptypes:
        v = Persistence.lifetime_in_secs(p)
        print('{}: {}'.format(p, v))

    print('SHORT_TERM: {}'.format(Persistence.lifetime_in_secs('SHORT_TERM')))

