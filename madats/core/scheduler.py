"""
`madats.core.scheduler`
====================================

.. currentmodule:: madats.core.scheduler

:platform: Unix, Mac
:synopsis: Module abstracting the job schedulers for managing batch scripts

.. moduleauthor:: Devarshi Ghoshal <dghoshal@lbl.gov>

"""

from madats.utils.config import slurm_config, pbs_config
import sys

class Scheduler(object):
    """
    Scheduler class that lists the different types of schedulers supported in MaDaTS
    """

    NONE = 0
    SLURM = 1
    PBS = 2

    scheduler_id = {'slurm': SLURM, 'pbs': PBS}
    scheduler_name = {SLURM: 'slurm', PBS: 'pbs'}
    scheduler_config = {SLURM: slurm_config, PBS: pbs_config}

    dependency_map = {NONE: '', SLURM: '--dependency=afterok:', PBS: '-W depend=afterok:'}
    delimiter_map = {NONE: '', SLURM: ',', PBS: ':'}

    @staticmethod
    def submit_command(scheduler):
        scheduler_config = Scheduler.scheduler_config.get(scheduler, None)
        if scheduler_config != None:
            return scheduler_config.submit
        else:
            return 'bash -c'

    @staticmethod
    def status_command(scheduler):
        scheduler_config = Scheduler.scheduler_config.get(scheduler, None)
        if scheduler_config != None:
            return scheduler_config.status
        else:
            return None

    @staticmethod
    def get_directive(scheduler, option):
        scheduler_config = Scheduler.scheduler_config.get(scheduler, None)
        if scheduler_config != None:
            if option in scheduler_config.directives:
                return scheduler_config.directives[option]
            else:
                return None
        else:
            return None

    @staticmethod
    def dependency_specifier(scheduler):
        return Scheduler.dependency_map.get(scheduler, '')

    @staticmethod
    def dependency_delimiter(scheduler):
        return Scheduler.delimiter_map.get(scheduler, '')

    @staticmethod
    def type(scheduler_name):
        return Scheduler.scheduler_id.get(scheduler_name, Scheduler.NONE)


