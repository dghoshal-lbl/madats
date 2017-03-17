from abstractions.scheduler_abstraction import AbstractScheduler

class PbsSchedulerManager(AbstractScheduler):
    def setopts(self, **kwargs):
        self.__directive__ = '#PBS'
        self.__run_cmd__ = 'aprun'
        self.__submit_cmd__ = 'qsub'
        self.__query_cmd__ = 'qstat' 
        self.__opts__ = {
            'cpus': '-N',
            'cpus_per_task': '-c',
            'walltime': '-t',
            'queue': '-q',
            'account': '-a',
            'job': '-j',
            'outlog': '-o',
            'errlog': '-e'
            }


