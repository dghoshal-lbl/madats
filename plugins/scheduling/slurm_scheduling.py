from abstractions.system_interface import AbstractScheduling

class SlurmScheduling(AbstractScheduling):

    def setopts(self, **kwargs):
        self.__directive__ = '#SBATCH'
        self.__run_cmd__ = 'srun'
        self.__submit_cmd__ = 'sbatch'
        self.__query_cmd__ = 'squeue' 
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

