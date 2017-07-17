from abstractions.system_interfaces import AbstractScheduling
from utils import dagman, helper
from madats.vds import Task
import uuid
import os
import sys

class MadatsScheduling(AbstractScheduling):

    def __slurm__(self):
        self.__directive__ = '#SBATCH'
        self.__run_cmd__ = 'srun'
        self.__submit_cmd__ = 'sbatch'
        self.__query_cmd__ = 'squeue' 
        self.__dependency_spec__ = '--dependency=afterok:'
        self.__dependency_delim__ = ','
        self.__awk_jobid__ = " | awk '{print $NF}'"
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
        self.__directive__ = '#PBS'
        self.__run_cmd__ = 'aprun'
        self.__submit_cmd__ = 'qsub'
        self.__query_cmd__ = 'qstat' 
        self.__dependency_spec__ = '-W depend=afterok:'
        self.__dependency_delim__ = ':'
        self.__awk_jobid__ = ""
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


    # TODO: implementation
    def __gen_code__(self, dag, async):
        submit_scripts = []
        tasks = dagman.batch_execution_order(dag)
        idx = 0
        for task in tasks:
            script = task.command + ' ' + ' '.join(task.params)
            idx += 1
            submit_scripts.append(script)
        return submit_scripts

    # TODO: implementation
    def __run__(self, command):
        print('Running command: {}'.format(command))
        return [0]

    def set(self, scheduler, **kwargs):
        scheduler_opts = {'slurm': self.__slurm__, 'pbs': self.__pbs__}
        if scheduler in scheduler_opts:
            scheduler_opts[scheduler]()
            self.__scheduler__ = scheduler
        else:
            print('Scheduler {} not implemented!'.format(scheduler))
        
        #self.pre_exec_stmts = pre_exec
        #self.post_exec_stmts = post_exec


    def __gen_jobscript__(self, tasks):
        submit_scripts = []
        submit_dir = os.path.abspath(str(uuid.uuid4()).replace('-', ''))
        if not os.path.exists(submit_dir):
            os.makedirs(submit_dir)
        id = 0
        for task in tasks:
            if task.type == Task.COMPUTE:
                job_id = 'task' + str(id)
            else:
                job_id = 'datatask' + str(id)
        
            job_file = os.path.join(submit_dir, job_id + '.' + self.__scheduler__)
            with open(job_file, 'w') as f:
                if task.walltime != '':
                    line = self.__directive__ + ' ' + self.__opts__['walltime'] + ' ' + task.walltime + '\n'
                    f.write(line)
                if task.cpus > 0:
                    line = self.__directive__ + ' ' + self.__opts__['cpus'] + ' ' + str(task.cpus) + '\n'
                    f.write(line)
                if task.queue != '':
                    line = self.__directive__ + ' ' + self.__opts__['queue'] + ' ' + task.queue + '\n'
                    f.write(line)
                if task.account != '':
                    line = self.__directive__ + ' ' + self.__opts__['account'] + ' ' + task.account + '\n'
                    f.write(line)
                line = self.__directive__ + ' ' + self.__opts__['job'] + ' ' + job_id + '\n'
                f.write(line)
                line = self.__directive__ + ' ' + self.__opts__['outlog'] + ' ' + job_id + '.out\n'
                f.write(line)
                line = self.__directive__ + ' ' + self.__opts__['errlog'] + ' ' + job_id + '.err\n'
                f.write(line)

                for stmt in task.pre_exec:
                    f.write(stmt + '\n')

                cmd = task.get_command() + ' ' + ' '.join(task.params) + '\n'
                f.write(cmd)

                for stmt in task.post_exec:
                    f.write(stmt + '\n')
            
            submit_scripts.append(os.path.basename(job_file))
            #submit_scripts.append(job_file)
            id += 1

        return submit_dir, submit_scripts


    def __gen_wrapper__(self, submit_dir, submit_scripts, tasks, dag):
        task_map = {}
        wrapper_script = os.path.join(submit_dir, 'submit.sh')        
        id = 0
        with open(wrapper_script, 'w') as f:
            line = '#!/bin/bash\n'        
            f.write(line)
            chdir = 'cd {}\n'.format(submit_dir)
            f.write(chdir)

            job_ids = []
            for task in tasks:
                if task.type == Task.COMPUTE:
                    job_id = 'task' + str(id)
                else:
                    job_id = 'datatask' + str(id)                
                task_map[task.__id__] = job_id
                preds = dagman.predecessors(dag, task)
                pred_ids = ['$'+task_map[pred.__id__] for pred in preds]
                if len(pred_ids) > 0:
                    dep_str = ' ' + self.__dependency_spec__ + self.__dependency_delim__.join(pred_ids) + ' '
                else:
                    dep_str = ' '
                line = job_id + '=`' + self.__submit_cmd__ + dep_str + submit_scripts[id] + self.__awk_jobid__ + '`\n' 
                f.write(line)                
                job_ids.append('$'+job_id)
                id += 1

            job_id_str = 'echo Jobs=' + ','.join(job_ids) + '\n'
            f.write(job_id_str)
        os.chmod(wrapper_script, 0744)
        return wrapper_script 
                

    def submit(self, dag):
        tasks = dagman.batch_execution_order(dag)
        submit_dir, submit_scripts = self.__gen_jobscript__(tasks)
        wrapper_script = self.__gen_wrapper__(submit_dir, submit_scripts, tasks, dag)
        submit_cmd = 'bash -c ' + wrapper_script
        status = helper.run(submit_cmd)
        return status


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
