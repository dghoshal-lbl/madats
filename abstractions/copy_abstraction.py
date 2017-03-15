'''
Data copy abstraction
'''

import abc

class CopyAbstract():
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        #print('Data copy abstraction')
        pass
        

    @abc.abstractmethod
    def copy(self, src, dest, keep_src=True):
        #print('Move data from src to dest using some protocol (scp, cp, gridftp etc.)')
        raise NotImplementedError()


    @abc.abstractmethod
    def copy_async(self, src, dest, keep_src=True):
        #print('Asynchronous move from src to dest')
        raise NotImplementedError()


    @abc.abstractmethod
    def poll(self, id):
        #print('Poll for an asynchronous move completion')
        raise NotImplementedError()
        
