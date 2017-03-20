'''
Storage management abstraction for application programmers and users to integrate with different storage systems
'''

import abc

class AbstractStorage():
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        #print('Storage management abstraction')
        #self.storage_hierarchy = None
        pass

    @abc.abstractmethod
    def get_hierarchy(self):
        #print('Query the memory/storage system to obtain memory/storage tiers')
        raise NotImplementedError()

    @abc.abstractmethod
    def get_id_path(self, storage_hierarchy, data_object):
        #print('Return storage-system identifier and relative path w.r.t. the mount point of the storage system')
        raise NotImplementedError()
        

    @abc.abstractmethod
    def get_storage_path(self, storage_hierarchy, storage_id):
        #print('Mount point of the storage-system identifier')
        raise NotImplementedError()


    @abc.abstractmethod
    def get_storage_id(self, storage_hierarchy, mount_point):
        #print('Return storage-system identifier corresponding to a mount-point')
        raise NotImplementedError()

#######################################################################################

'''
Execution manager abstraction
'''

class AbstractExecution():
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        #print('Execution manager abstraction')
        pass

    @abc.abstractmethod
    def execute(self, dag, async_mode):
        raise NotImplementedError()

    @abc.abstractmethod
    def wait(self, execution_id):
        raise NotImplementedError()

    @abc.abstractmethod
    def status(self, execution_id):
        raise NotImplementedError()

#############################################################################################

'''
Batch scheduling abstraction
'''

class AbstractScheduling():
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.__scheduler__ = {
            'slurm': self.__slurm__,
            'pbs': self.__pbs__
            }
        
        self.__opts__ = {}
        self.__directive__ = ''
        self.__run_cmd__ = ''
        self.__submit_cmd__ = ''
        self.__query_cmd__ = ''
        self.__scheduler_opts__ = {}


    def set(self, scheduler, **kwargs):
        self.__scheduler__[scheduler]()
        self.__scheduler_opts__ = {
            'directive': self.__directive__,
            'run_cmd': self.__run_cmd__,
            'submit_cmd': self.__submit_cmd__,
            'query_cmd': self.__query_cmd__,
            'opts': self.__opts__,
            'extras': extraopts
            }
        for k in kwargs:
            self.__scheduler_opts__[k] = kwargs[k]


    @abc.abstractmethod
    def submit(self, dag, async_mode):
        """
        1. create a temp submission directory
        2. create job scripts for submission
        3. submit job scripts
        4. return the job submission id
        """
        pass

    @abc.abstractmethod
    def wait(self, job_id):
        """
        blocking wait for the job to finish
        """
        pass

    @abc.abstractmethod
    def status(self, job_id):
        """
        get status of the job
        """
        pass

    '''
    @abc.abstractmethod
    def setopts(self, **kwargs):
        raise NotImplementedError()
    '''
############################################################################################

'''
Data copy abstraction
'''

class AbstractCopy():
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        #print('Data copy abstraction')
        pass
        
    # TODO: complete implementation
    def run(self, command):
        print('Running copy command: {}'.format(command))

    @abc.abstractmethod
    def copy(self, src, dest, async=False, keep_src=True, **kwargs):
        #print('Move data from src to dest using some protocol (scp, cp, gridftp etc.)')
        raise NotImplementedError()


    @abc.abstractmethod
    def poll(self, id, **kwargs):
        #print('Poll for an asynchronous move completion')
        raise NotImplementedError()
        
