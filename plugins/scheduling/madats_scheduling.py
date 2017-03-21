from abstractions.system_interfaces import AbstractScheduling
from utils import dagman

class MadatsScheduling(AbstractScheduling):

    def __slurm__(self):
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


    def __pbs__(self):
        pass


    # TODO: implementation
    def __gen_code__(self, dag, async_mode):
        submit_scripts = []
        task_bins = dagman.bin_execution_order(dag)
        idx = 0
        for task_bin in task_bins:
            bin = []
            for task in task_bin:
                script = task.command + ' ' + ' '.join(task.params)
                bin.append(script)
            script_name = 'bin' + str(idx) + ': ' + ','.join(bin)
            idx += 1
            submit_scripts.append(script_name)
        return submit_scripts

    # TODO: implementation
    def __run__(self, command):
        print('Running command: {}'.format(command))
        return 0

    def set(self, scheduler, **kwargs):
        scheduler_opts = {'slurm': self.__slurm__, 'pbs': self.__pbs__}
        if scheduler in scheduler_opts:
            scheduler_opts[scheduler]()
            self.__scheduler__ = scheduler
        else:
            print('Scheduler {} not implemented!'.format(scheduler))
        
        #self.pre_exec_stmts = pre_exec
        #self.post_exec_stmts = post_exec


    def submit(self, dag, async_mode):
        submit_scripts = self.__gen_code__(dag, async_mode)
        submit_ids = []
        if async_mode == True:
            for submit_script in submit_scripts:
                submit_cmd = self.__submit_cmd__ + ' ' + submit_script
                submit_id = self.__run__(submit_cmd)
                submit_ids.append(submit_id)
        else:
            for submit_script in submit_scripts:
                submit_cmd = 'bash -c ' + submit_script
                submit_id = self.__run__(submit_cmd)
                submit_ids.append(submit_id)
        
        return submit_ids


    # TODO: implementation
    def wait(self, submit_ids):
        pass


    # TODO: implementation
    def status(self, submit_ids):
        status = 0
        for submit_id in submit_ids:
            if submit_id != 0:
                status = submit_id
                return status
        return status
