"""
`tests.test_madats`
====================================

.. currentmodule:: tests.test_madats

:platform: Unix, Mac
:synopsis: Unit test module

.. moduleauthor:: Devarshi Ghoshal <dghoshal@lbl.gov>

"""

import pytest
import subprocess
import os
import sys
import shutil
import madats
import random
import string
import yaml

class Tester():
    def setup(self):
        if 'MADATS_HOME' in os.environ:
            pass
        else:
            print('MADATS_HOME is not set!')
            sys.exit()
        madats_home = os.path.expandvars('$MADATS_HOME')
        self.workdir = os.path.join(madats_home, '_tmp')
        self.scratch = os.path.join(self.workdir, 'scratch')
        self.burst = os.path.join(self.workdir, 'burst')
        self.archive = os.path.join(self.workdir, 'archive')        
        
        if not os.path.exists(self.scratch):
            os.makedirs(self.scratch)
        if not os.path.exists(self.burst):
            os.makedirs(self.burst)
        if not os.path.exists(self.archive):
            os.makedirs(self.archive)

        self.__setup_storage_config__()
        

    def __setup_storage_config__(self):
        storage_config = {'system': 'test'}
        storage_config['test'] = {}
        storage_tiers = {'scratch': [self.scratch, 'ShortTerm', 700],
                         'burst': [self.burst, 'None', 1600], 
                         'archive': [self.archive, 'LongTerm', 1]}
        for k in storage_tiers:
            storage_config['test'][k] = {'mount': storage_tiers[k][0],
                                         'persist': storage_tiers[k][1],
                                         'bandwidth': storage_tiers[k][2]}

        storage_yaml = os.path.expandvars('$MADATS_HOME/config/storage.yaml')
        self.__write_yaml__(storage_config, storage_yaml)


    def teardown(self):
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir)
        pass


    def __create_file__(self, filepath, content=None):
        if not content:
            data = filepath
        else:
            data = content

        with open(filepath, 'w') as f:
            f.write(data)


    def __get_random_string__(self):
        random_str = ''.join([random.choice(string.ascii_letters + string.digits) for n in range(32)])
        return random_str


    def __get_workflow_dict__(self, ntasks, commands, task_params, vins, vouts):
        workflow = {}
        for i in range(ntasks):
            task = 'task' + str(i)
            workflow[task] = {}
            if len(vins[i]) > 0:
                workflow[task]['vin'] = vins[i]
            if len(vouts[i]) > 0:
                workflow[task]['vout'] = vouts[i]
            workflow[task]['params'] = task_params[i]
            workflow[task]['command'] = commands[i]

        return workflow


    def __write_yaml__(self, data, yaml_file):
        with open(yaml_file, 'w') as f:
            yaml.dump(data, f, default_flow_style=False)
        #return yaml_file


    def __get_file_data__(self, filename):
        with open(filename, 'r') as f:
            lines = f.readlines()
            return ''.join(lines)

    '''
    TEST-1: Create a workflow yaml and manage the workflow data through Madats  
    '''
    def test_workflow_yaml(self):
        test_name = 'test_workflow_yaml'
        ntasks = 2
        commands = ['cat', 'cat']
        datadir = os.path.join(self.scratch, test_name)
        if not os.path.exists(datadir):
            os.makedirs(datadir)
        files = [os.path.join(datadir, 'in1'), os.path.join(datadir, 'in2'),
                 os.path.join(datadir, 'inout1')]
        vins = [[files[0], files[1]], [files[2]]]
        vouts = [[files[2]], []]
        nfiles = len(files)
        strdata = []
        for i in range(nfiles - 1):
            strdata.append(self.__get_random_string__())
            self.__create_file__(files[i], strdata[i])
        task_params = [[vins[0][0], vins[0][1], '>', vouts[0][0]], [vouts[0][0]]]
        workflow = self.__get_workflow_dict__(ntasks, commands, task_params, vins, vouts)
        yaml_file = os.path.join(self.workdir, 'test.yaml')
        self.__write_yaml__(workflow, yaml_file)
        vds = madats.map(yaml_file)
        madats.manage(vds)
        output = self.__get_file_data__(vouts[0][0])
        input = ''.join(strdata)
        assert("{}".format(input) == output)


    '''
    TEST-2: Create a workflow dictionary obejct and manage the workflow data through Madats  
    '''
    def test_dict_obj(self):
        test_name = 'test_dict_obj'
        ntasks = 2
        commands = ['cat', 'cat']
        datadir = os.path.join(self.scratch, test_name)
        if not os.path.exists(datadir):
            os.makedirs(datadir)
        files = [os.path.join(datadir, 'in1'), os.path.join(datadir, 'in2'),
                 os.path.join(datadir, 'inout1')]
        vins = [[files[0], files[1]], [files[2]]]
        vouts = [[files[2]], []]
        nfiles = len(files)
        strdata = []
        for i in range(nfiles - 1):
            strdata.append(self.__get_random_string__())
            self.__create_file__(files[i], strdata[i])
        task_params = [[vins[0][0], vins[0][1], '>', vouts[0][0]], [vouts[0][0]]]
        workflow = self.__get_workflow_dict__(ntasks, commands, task_params, vins, vouts)
        vds = madats.map(workflow, language='DictObj')
        madats.manage(vds)
        output = self.__get_file_data__(vouts[0][0])
        input = ''.join(strdata)
        assert("{}".format(input) == output)


    '''
    TEST-3: Manage a workflow by using the data-driven API in madats that allows
            users to create virtual data objects and associate tasks for automated
            data management
    '''
    def test_api_simple(self):
        test_name = 'test_api_simple'
        datadir = os.path.join(self.scratch, test_name)
        if not os.path.exists(datadir):
            os.makedirs(datadir)
        strdata = []
        files = [os.path.join(datadir, 'in1'), os.path.join(datadir, 'in2'),
                 os.path.join(datadir, 'inout1')]
        for i in range(len(files) - 1):
            strdata.append(self.__get_random_string__())
            self.__create_file__(files[i], strdata[i])

        # create a VDS
        vds = madats.VirtualDataSpace()
        
        # create VDOs
        vdo1 = madats.VirtualDataObject(files[0])
        vdo2 = madats.VirtualDataObject(files[1])
        vdo3 = madats.VirtualDataObject(files[2])

        # create tasks
        task1 = madats.Task(command='cat')
        task1.params = [vdo1, vdo2, '>', vdo3]
        task2 = madats.Task(command='cat')
        task2.params = [vdo3]
    
        # define VDO and task associations
        vdo1.consumers = [task1]
        vdo2.consumers = [task1]
        vdo3.producers = [task1]
        vdo3.consumers = [task2]
        vds.add(vdo1)
        vds.add(vdo2)
        vds.add(vdo3)
        
        # manage VDS
        madats.manage(vds)

        output = self.__get_file_data__(files[2])
        input = ''.join(strdata)
        assert("{}".format(input) == output)
        

    '''
    TEST-4: Manage a workflow using the `workflow-aware` data management strategy in Madats
    '''
    def test_workflow_aware(self):
        #vds.data_management_policy = Policy.STORAGE_AWARE
        #vds.auto_cleanup = True
        #vdo.non_movable = True
        #vdo.persistence = Persistence.LONG_TERM
        test_name = 'test_workflow_aware'
        datadir = os.path.join(self.scratch, test_name)
        if not os.path.exists(datadir):
            os.makedirs(datadir)
        strdata = []
        files = [os.path.join(datadir, 'in1'), os.path.join(datadir, 'in2'),
                 os.path.join(datadir, 'inout1')]
        for i in range(len(files) - 1):
            strdata.append(self.__get_random_string__())
            self.__create_file__(files[i], strdata[i])

        # create a VDS
        vds = madats.VirtualDataSpace()
        vds.strategy = madats.Policy.WORKFLOW_AWARE
        
        # create VDOs
        vdo1 = madats.VirtualDataObject(files[0])
        vdo2 = madats.VirtualDataObject(files[1])
        vdo3 = madats.VirtualDataObject(files[2])

        # create tasks
        task1 = madats.Task(command='cat')
        task1.params = [vdo1, vdo2, '>', vdo3]
        task2 = madats.Task(command='cat')
        task2.params = [vdo3]
    
        # define VDO and task associations
        vdo1.consumers = [task1]
        vdo2.consumers = [task1]
        vdo3.producers = [task1]
        vdo3.consumers = [task2]
        vds.add(vdo1)
        vds.add(vdo2)
        vds.add(vdo3)
        
        # manage VDS
        madats.manage(vds)

        stagein1 = os.path.join(self.burst, test_name, 'in1')
        stagein2 = os.path.join(self.burst, test_name, 'in2')
        intfile = os.path.join(self.burst, test_name, 'inout1')
        # since it's workflow-aware, it only creates the intermediate file on fast burst tier
        # there is no final output of the workflow as the second task only takes i/p and no o/p
        assert(os.path.exists(intfile))
        assert(not os.path.exists(stagein1))
        assert(not os.path.exists(stagein2))
        output = self.__get_file_data__(intfile)
        input = ''.join(strdata)
        assert("{}".format(input) == output)

    
    '''
    TEST-5: Manage a workflow using the `storage-aware` data management strategy in Madats
    '''
    def test_storage_aware(self):
        test_name = 'test_storage_aware'
        datadir = os.path.join(self.scratch, test_name)
        if not os.path.exists(datadir):
            os.makedirs(datadir)
        strdata = []
        files = [os.path.join(datadir, 'in1'), os.path.join(datadir, 'in2'),
                 os.path.join(datadir, 'inout1')]
        for i in range(len(files) - 1):
            strdata.append(self.__get_random_string__())
            self.__create_file__(files[i], strdata[i])

        # create a VDS
        vds = madats.VirtualDataSpace()
        vds.strategy = madats.Policy.STORAGE_AWARE
        
        # create VDOs
        vdo1 = madats.VirtualDataObject(files[0])
        vdo2 = madats.VirtualDataObject(files[1])
        vdo3 = madats.VirtualDataObject(files[2])

        # create tasks
        task1 = madats.Task(command='cat')
        task1.params = [vdo1, vdo2, '>', vdo3]
        task2 = madats.Task(command='cat')
        task2.params = [vdo3]
    
        # define VDO and task associations
        vdo1.consumers = [task1]
        vdo2.consumers = [task1]
        vdo3.producers = [task1]
        vdo3.consumers = [task2]
        vds.add(vdo1)
        vds.add(vdo2)
        vds.add(vdo3)
        
        # manage VDS
        madats.manage(vds)

        stagein1 = os.path.join(self.burst, test_name, 'in1')
        stagein2 = os.path.join(self.burst, test_name, 'in2')
        intfile = os.path.join(self.burst, test_name, 'inout1')
        # since it's storage-aware, it creates the intermediate file and stages in all the
        # inputs on fast burst tier
        # there is no final output of the workflow as the second task only takes i/p and no o/p
        assert(os.path.exists(intfile))
        assert(os.path.exists(stagein1))
        assert(os.path.exists(stagein2))
        output = self.__get_file_data__(intfile)
        input = ''.join(strdata)
        assert("{}".format(input) == output)


    '''
    TEST-6: Manage a workflow with multiple intermediate outputs using the
            `workflow-aware` data management strategy in Madats
    '''
    def test_workflow_aware2(self):
        test_name = 'test_workflow_aware2'
        datadir = os.path.join(self.scratch, test_name)
        if not os.path.exists(datadir):
            os.makedirs(datadir)
        strdata = []
        files = [os.path.join(datadir, 'in1'), os.path.join(datadir, 'in2'),
                 os.path.join(datadir, 'inout1'), os.path.join(datadir, 'inout2'),
                 os.path.join(datadir, 'out')]
        for i in range(len(files) - 3):
            strdata.append(self.__get_random_string__())
            self.__create_file__(files[i], strdata[i])

        # create a VDS
        vds = madats.VirtualDataSpace()
        vds.strategy = madats.Policy.WORKFLOW_AWARE
        
        # create VDOs
        vdo1 = madats.VirtualDataObject(files[0])
        vdo2 = madats.VirtualDataObject(files[1])
        vdo3 = madats.VirtualDataObject(files[2])
        vdo4 = madats.VirtualDataObject(files[3])
        vdo5 = madats.VirtualDataObject(files[4])

        # create tasks
        task1 = madats.Task(command='cat')
        task1.params = [vdo1, vdo2, '>', vdo3]
        task2 = madats.Task(command='cat')
        task2.params = [vdo3, '>', vdo4]
        task3 = madats.Task(command='cat')
        task3.params = [vdo4, '>', vdo5]
    
        # define VDO and task associations
        vdo1.consumers = [task1]
        vdo2.consumers = [task1]
        vdo3.producers = [task1]
        vdo3.consumers = [task2]
        vdo4.producers = [task2]
        vdo4.consumers = [task3]
        vdo5.producers = [task3]
        vds.add(vdo1)
        vds.add(vdo2)
        vds.add(vdo3)
        vds.add(vdo4)
        vds.add(vdo5)
        
        # manage VDS
        madats.manage(vds)

        stagein1 = os.path.join(self.burst, test_name, 'in1')
        stagein2 = os.path.join(self.burst, test_name, 'in2')
        intfile1 = os.path.join(self.burst, test_name, 'inout1')
        intfile2 = os.path.join(self.burst, test_name, 'inout2')
        finalout = os.path.join(self.burst, test_name, 'out')
        # since it's workflow-aware, only the intermediate files are created on fast burst tier
        assert(os.path.exists(intfile1))
        assert(os.path.exists(intfile2))
        assert(not os.path.exists(stagein1))
        assert(not os.path.exists(stagein2))
        assert(not os.path.exists(finalout))
        assert(not os.path.exists(files[2]))
        assert(not os.path.exists(files[3]))
        output = self.__get_file_data__(files[4])
        input = ''.join(strdata)
        assert("{}".format(input) == output)


    '''
    TEST-7: Manage a workflow with multiple intermediate outputs using the
            `storage-aware` data management strategy in Madats
    '''
    def test_storage_aware2(self):
        test_name = 'test_storage_aware2'
        datadir = os.path.join(self.scratch, test_name)
        if not os.path.exists(datadir):
            os.makedirs(datadir)
        strdata = []
        files = [os.path.join(datadir, 'in1'), os.path.join(datadir, 'in2'),
                 os.path.join(datadir, 'inout1'), os.path.join(datadir, 'inout2'),
                 os.path.join(datadir, 'out')]
        for i in range(len(files) - 3):
            strdata.append(self.__get_random_string__())
            self.__create_file__(files[i], strdata[i])

        # create a VDS
        vds = madats.VirtualDataSpace()
        vds.strategy = madats.Policy.STORAGE_AWARE
        
        # create VDOs
        vdo1 = madats.VirtualDataObject(files[0])
        vdo2 = madats.VirtualDataObject(files[1])
        vdo3 = madats.VirtualDataObject(files[2])
        vdo4 = madats.VirtualDataObject(files[3])
        vdo5 = madats.VirtualDataObject(files[4])

        # create tasks
        task1 = madats.Task(command='cat')
        task1.params = [vdo1, vdo2, '>', vdo3]
        task2 = madats.Task(command='cat')
        task2.params = [vdo3, '>', vdo4]
        task3 = madats.Task(command='cat')
        task3.params = [vdo4, '>', vdo5]
    
        # define VDO and task associations
        vdo1.consumers = [task1]
        vdo2.consumers = [task1]
        vdo3.producers = [task1]
        vdo3.consumers = [task2]
        vdo4.producers = [task2]
        vdo4.consumers = [task3]
        vdo5.producers = [task3]
        vds.add(vdo1)
        vds.add(vdo2)
        vds.add(vdo3)
        vds.add(vdo4)
        vds.add(vdo5)
        
        # manage VDS
        madats.manage(vds)

        stagein1 = os.path.join(self.burst, test_name, 'in1')
        stagein2 = os.path.join(self.burst, test_name, 'in2')
        intfile1 = os.path.join(self.burst, test_name, 'inout1')
        intfile2 = os.path.join(self.burst, test_name, 'inout2')
        finalout = os.path.join(self.burst, test_name, 'out')
        # since it's storage-aware, it creates the intermediate file and stages in all the
        # inputs on fast burst tier
        assert(os.path.exists(intfile1))
        assert(os.path.exists(intfile2))
        assert(os.path.exists(stagein1))
        assert(os.path.exists(stagein2))
        assert(not os.path.exists(files[2]))
        assert(not os.path.exists(files[3]))
        assert(os.path.exists(finalout))
        output = self.__get_file_data__(files[4])
        input = ''.join(strdata)
        assert("{}".format(input) == output)

    
    '''
    TEST-8: Manage a workflow by enabling auto-cleanup in Madats
    '''
    def test_auto_cleanup(self):
        test_name = 'test_auto_cleanup'
        datadir = os.path.join(self.scratch, test_name)
        if not os.path.exists(datadir):
            os.makedirs(datadir)
        strdata = []
        files = [os.path.join(datadir, 'in1'), os.path.join(datadir, 'in2'),
                 os.path.join(datadir, 'inout'), os.path.join(datadir, 'out')]
        for i in range(len(files) - 2):
            strdata.append(self.__get_random_string__())
            self.__create_file__(files[i], strdata[i])

        # create a VDS
        vds = madats.VirtualDataSpace()
        vds.strategy = madats.Policy.STORAGE_AWARE
        vds.auto_cleanup = True
        
        # create VDOs
        vdo1 = madats.VirtualDataObject(files[0])
        vdo2 = madats.VirtualDataObject(files[1])
        vdo3 = madats.VirtualDataObject(files[2])
        vdo4 = madats.VirtualDataObject(files[3])

        # create tasks
        task1 = madats.Task(command='cat')
        task1.params = [vdo1, vdo2, '>', vdo3]
        task2 = madats.Task(command='cat')
        task2.params = [vdo3, '>', vdo4]
    
        # define VDO and task associations
        vdo1.consumers = [task1]
        vdo2.consumers = [task1]
        vdo3.producers = [task1]
        vdo3.consumers = [task2]
        vdo4.producers = [task2]
        vds.add(vdo1)
        vds.add(vdo2)
        vds.add(vdo3)
        vds.add(vdo4)
        
        # manage VDS
        madats.manage(vds)

        stagein1 = os.path.join(self.burst, test_name, 'in1')
        stagein2 = os.path.join(self.burst, test_name, 'in2')
        intfile = os.path.join(self.burst, test_name, 'inout')
        # since it's storage-aware with auto-cleanup, it creates the intermediate file
        # and stages in all the inputs on fast burst tier and then deletes them as they are used
        # there is no final output of the workflow as the second task only takes i/p and no o/p
        assert(not os.path.exists(intfile))
        assert(not os.path.exists(stagein1))
        assert(not os.path.exists(stagein2))
        assert(not os.path.exists(files[2]))
        output = self.__get_file_data__(files[3])
        input = ''.join(strdata)
        assert("{}".format(input) == output)

    
    '''
    TEST-9: Manage a workflow by forcing an intermediate output to be persistent through Madats
    '''
    def test_persistent_vdo(self):
        test_name = 'test_persistent_vdo'
        datadir = os.path.join(self.scratch, test_name)
        if not os.path.exists(datadir):
            os.makedirs(datadir)
        strdata = []
        files = [os.path.join(datadir, 'in1'), os.path.join(datadir, 'in2'),
                 os.path.join(datadir, 'inout'), os.path.join(datadir, 'out')]
        for i in range(len(files) - 2):
            strdata.append(self.__get_random_string__())
            self.__create_file__(files[i], strdata[i])

        # create a VDS
        vds = madats.VirtualDataSpace()
        vds.strategy = madats.Policy.STORAGE_AWARE
        vds.auto_cleanup = True
        
        # create VDOs
        vdo1 = madats.VirtualDataObject(files[0])
        vdo2 = madats.VirtualDataObject(files[1])
        vdo3 = madats.VirtualDataObject(files[2])
        vdo4 = madats.VirtualDataObject(files[3])
        vdo3.persistence = madats.Persistence.LONG_TERM
        #vdo3.persist = True

        # create tasks
        task1 = madats.Task(command='cat')
        task1.params = [vdo1, vdo2, '>', vdo3]
        task2 = madats.Task(command='cat')
        task2.params = [vdo3, '>', vdo4]
    
        # define VDO and task associations
        vdo1.consumers = [task1]
        vdo2.consumers = [task1]
        vdo3.producers = [task1]
        vdo3.consumers = [task2]
        vdo4.producers = [task2]
        vds.add(vdo1)
        vds.add(vdo2)
        vds.add(vdo3)
        vds.add(vdo4)
        
        # manage VDS
        madats.manage(vds)

        stagein1 = os.path.join(self.burst, test_name, 'in1')
        stagein2 = os.path.join(self.burst, test_name, 'in2')
        intfile = os.path.join(self.burst, test_name, 'inout')
        # since it's storage-aware with auto-cleanup, it creates the intermediate file
        # and stages in all the inputs on fast burst tier and then deletes them as they are used
        # except for the once that is made persistent
        # there is no final output of the workflow as the second task only takes i/p and no o/p
        assert(not os.path.exists(intfile))
        assert(not os.path.exists(stagein1))
        assert(not os.path.exists(stagein2))
        assert(os.path.exists(files[2]))
        output = self.__get_file_data__(files[3])
        input = ''.join(strdata)
        assert("{}".format(input) == output)

    
    '''
    TEST-10: Manage a workflow with one non-movable vdo through Madats
    '''
    def test_nonmovable_vdo(self):
        test_name = 'test_nonmovable_vdo'
        datadir = os.path.join(self.scratch, test_name)
        if not os.path.exists(datadir):
            os.makedirs(datadir)
        strdata = []
        files = [os.path.join(datadir, 'in1'), os.path.join(datadir, 'in2'),
                 os.path.join(datadir, 'inout'), os.path.join(datadir, 'out')]
        for i in range(len(files) - 2):
            strdata.append(self.__get_random_string__())
            self.__create_file__(files[i], strdata[i])

        # create a VDS
        vds = madats.VirtualDataSpace()
        vds.strategy = madats.Policy.STORAGE_AWARE
        
        # create VDOs
        vdo1 = madats.VirtualDataObject(files[0])
        vdo2 = madats.VirtualDataObject(files[1])
        vdo3 = madats.VirtualDataObject(files[2])
        vdo4 = madats.VirtualDataObject(files[3])
        vdo2.non_movable = True

        # create tasks
        task1 = madats.Task(command='cat')
        task1.params = [vdo1, vdo2, '>', vdo3]
        task2 = madats.Task(command='cat')
        task2.params = [vdo3, '>', vdo4]
    
        # define VDO and task associations
        vdo1.consumers = [task1]
        vdo2.consumers = [task1]
        vdo3.producers = [task1]
        vdo3.consumers = [task2]
        vdo4.producers = [task2]
        vds.add(vdo1)
        vds.add(vdo2)
        vds.add(vdo3)
        vds.add(vdo4)
        
        # manage VDS
        madats.manage(vds)

        stagein1 = os.path.join(self.burst, test_name, 'in1')
        stagein2 = os.path.join(self.burst, test_name, 'in2')
        intfile = os.path.join(self.burst, test_name, 'inout')
        finalout = os.path.join(self.burst, test_name, 'out')
        # the vdo that is made non-movable will not be moved to another tier even
        # if the data management strategy is defined on the vds
        # there is no final output of the workflow as the second task only takes i/p and no o/p
        assert(os.path.exists(intfile))
        assert(os.path.exists(stagein1))
        assert(not os.path.exists(stagein2))
        assert(not os.path.exists(files[2]))
        assert(os.path.exists(finalout))
        output = self.__get_file_data__(files[3])
        input = ''.join(strdata)
        assert("{}".format(input) == output)

    
    '''
    TEST-11: Manage a workflow for which some input data is already staged in
    '''
    def test_already_moved(self):
        test_name = 'test_already_moved'
        datadir = os.path.join(self.scratch, test_name)
        if not os.path.exists(datadir):
            os.makedirs(datadir)
        strdata = []
        files = [os.path.join(datadir, 'in1'), os.path.join(datadir, 'in2'),
                 os.path.join(datadir, 'inout'), os.path.join(datadir, 'out')]
        for i in range(len(files) - 2):
            strdata.append(self.__get_random_string__())
            self.__create_file__(files[i], strdata[i])

        # create a VDS
        vds = madats.VirtualDataSpace()
        vds.strategy = madats.Policy.STORAGE_AWARE
        
        # create VDOs
        vdo1 = madats.VirtualDataObject(files[0])
        vdo2 = madats.VirtualDataObject(files[1])
        vdo3 = madats.VirtualDataObject(files[2])
        vdo4 = madats.VirtualDataObject(files[3])

        # create tasks
        task1 = madats.Task(command='cat')
        task1.params = [vdo1, vdo2, '>', vdo3]
        task2 = madats.Task(command='cat')
        task2.params = [vdo3, '>', vdo4]
    
        # define VDO and task associations
        vdo1.consumers = [task1]
        vdo2.consumers = [task1]
        vdo3.producers = [task1]
        vdo3.consumers = [task2]
        vdo4.producers = [task2]
        vds.add(vdo1)
        vds.add(vdo2)
        vds.add(vdo3)
        vds.add(vdo4)
        
        stagein1 = os.path.join(self.burst, test_name, 'in1')
        if not os.path.exists(os.path.join(self.burst, test_name)):
            os.makedirs(os.path.join(self.burst, test_name))
        shutil.copyfile(files[0], stagein1)
        tm1 = os.path.getmtime(stagein1)
        # manage VDS
        madats.manage(vds)
        tm2 = os.path.getmtime(stagein1)
        # the file modification time should not change during `manage` because
        # the same file will not be moved, if not modified
        assert(tm1 == tm2)

        stagein2 = os.path.join(self.burst, test_name, 'in2')
        intfile = os.path.join(self.burst, test_name, 'inout')
        finalout = os.path.join(self.burst, test_name, 'out')
        assert(os.path.exists(intfile))
        assert(os.path.exists(stagein1))
        assert(os.path.exists(stagein2))
        assert(not os.path.exists(files[2]))
        assert(os.path.exists(finalout))
        output = self.__get_file_data__(files[3])
        input = ''.join(strdata)
        assert("{}".format(input) == output)

    
    '''
    TEST-12: Manage a workflow with duplicate vdos added to the vds
    '''
    def test_duplicate_vdos(self):
        test_name = 'test_duplicate_vdos'
        datadir = os.path.join(self.scratch, test_name)
        if not os.path.exists(datadir):
            os.makedirs(datadir)
        strdata = []
        files = [os.path.join(datadir, 'in1'), os.path.join(datadir, 'in2'),
                 os.path.join(datadir, 'inout'), os.path.join(datadir, 'out')]
        for i in range(len(files) - 2):
            strdata.append(self.__get_random_string__())
            self.__create_file__(files[i], strdata[i])

        # create a VDS
        vds = madats.VirtualDataSpace()
        vds.strategy = madats.Policy.STORAGE_AWARE
        
        # create VDOs
        vdo1 = madats.VirtualDataObject(files[0])
        vdo2 = madats.VirtualDataObject(files[1])
        vdo3 = madats.VirtualDataObject(files[2])
        vdo4 = madats.VirtualDataObject(files[3])

        # create tasks
        task1 = madats.Task(command='cat')
        task1.params = [vdo1, vdo2, '>', vdo3]
        task2 = madats.Task(command='cat')
        task2.params = [vdo3, '>', vdo4]
    
        # define VDO and task associations
        vdo1.consumers = [task1]
        vdo2.consumers = [task1]
        vdo3.producers = [task1]
        vdo3.consumers = [task2]
        vdo4.producers = [task2]
        vds.add(vdo1)
        vds.add(vdo1)
        vds.add(vdo2)
        vds.add(vdo3)
        vds.add(vdo4)

        # vdo1 should not be added twice
        assert(len(vds.vdos) == 4)
        # manage VDS
        madats.manage(vds)

        stagein1 = os.path.join(self.burst, test_name, 'in1')
        stagein2 = os.path.join(self.burst, test_name, 'in2')
        intfile = os.path.join(self.burst, test_name, 'inout')
        finalout = os.path.join(self.burst, test_name, 'out')
        assert(os.path.exists(intfile))
        assert(os.path.exists(stagein1))
        assert(os.path.exists(stagein2))
        assert(not os.path.exists(files[2]))
        assert(os.path.exists(finalout))
        output = self.__get_file_data__(files[3])
        input = ''.join(strdata)
        assert("{}".format(input) == output)


    '''
    TEST-13: Manage a workflow with a vdo having no tasks
    '''
    def test_vdo_no_tasks(self):
        test_name = 'test_vdo_no_tasks'
        datadir = os.path.join(self.scratch, test_name)
        if not os.path.exists(datadir):
            os.makedirs(datadir)
        strdata = []
        files = [os.path.join(datadir, 'in1'), os.path.join(datadir, 'in2'),
                 os.path.join(datadir, 'inout'), os.path.join(datadir, 'out'),
                 os.path.join(datadir, 'nouse')]
        for i in range(len(files) - 3):
            strdata.append(self.__get_random_string__())
            self.__create_file__(files[i], strdata[i])

        # create a VDS
        vds = madats.VirtualDataSpace()
        vds.strategy = madats.Policy.STORAGE_AWARE
        
        # create VDOs
        vdo1 = madats.VirtualDataObject(files[0])
        vdo2 = madats.VirtualDataObject(files[1])
        vdo3 = madats.VirtualDataObject(files[2])
        vdo4 = madats.VirtualDataObject(files[3])
        vdo5 = madats.VirtualDataObject(files[4])

        # create tasks
        task1 = madats.Task(command='cat')
        task1.params = [vdo1, vdo2, '>', vdo3]
        task2 = madats.Task(command='cat')
        task2.params = [vdo3, '>', vdo4]
    
        # define VDO and task associations
        vdo1.consumers = [task1]
        vdo2.consumers = [task1]
        vdo3.producers = [task1]
        vdo3.consumers = [task2]
        vdo4.producers = [task2]
        vds.add(vdo1)
        vds.add(vdo2)
        vds.add(vdo3)
        vds.add(vdo4)
        vds.add(vdo5)

        assert(len(vds.vdos) == 5)
        # manage VDS
        madats.manage(vds)

        stagein1 = os.path.join(self.burst, test_name, 'in1')
        stagein2 = os.path.join(self.burst, test_name, 'in2')
        intfile = os.path.join(self.burst, test_name, 'inout')
        finalout = os.path.join(self.burst, test_name, 'out')
        nouse = os.path.join(self.burst, test_name, 'nouse')
        assert(os.path.exists(intfile))
        assert(os.path.exists(stagein1))
        assert(os.path.exists(stagein2))
        assert(not os.path.exists(files[2]))
        # since there is no associated tasks to the vdo, madats neither checks for
        # the data nor does it create create any task to copy/move the data
        # hence it does not exist on any storage tier
        assert(not os.path.exists(nouse))
        assert(not os.path.exists(files[4]))
        assert(os.path.exists(finalout))
        output = self.__get_file_data__(files[3])
        input = ''.join(strdata)
        assert("{}".format(input) == output)


    '''
    TEST-14: Manage a workflow by not adding a vdo
    '''
    def test_vdo_not_added(self):
        test_name = 'test_vdo_not_added'
        datadir = os.path.join(self.scratch, test_name)
        if not os.path.exists(datadir):
            os.makedirs(datadir)
        strdata = []
        files = [os.path.join(datadir, 'in1'), os.path.join(datadir, 'in2'),
                 os.path.join(datadir, 'inout'), os.path.join(datadir, 'out')]
        for i in range(len(files) - 2):
            strdata.append(self.__get_random_string__())
            self.__create_file__(files[i], strdata[i])

        # create a VDS
        vds = madats.VirtualDataSpace()
        vds.strategy = madats.Policy.STORAGE_AWARE
        
        # create VDOs
        vdo1 = madats.VirtualDataObject(files[0])
        vdo2 = madats.VirtualDataObject(files[1])
        vdo3 = madats.VirtualDataObject(files[2])
        vdo4 = madats.VirtualDataObject(files[3])

        # create tasks
        task1 = madats.Task(command='cat')
        task1.params = [vdo1, vdo2, '>', vdo3]
        task2 = madats.Task(command='cat')
        task2.params = [vdo3, '>', vdo4]
    
        # define VDO and task associations
        vdo1.consumers = [task1]
        vdo2.consumers = [task1]
        vdo3.producers = [task1]
        vdo3.consumers = [task2]
        vdo4.producers = [task2]
        vds.add(vdo1)
        vds.add(vdo2)
        vds.add(vdo3)
        #vds.add(vdo4)

        #assert(len(vds.vdos) == 4)
        # manage VDS
        madats.manage(vds)

        stagein1 = os.path.join(self.burst, test_name, 'in1')
        stagein2 = os.path.join(self.burst, test_name, 'in2')
        intfile = os.path.join(self.burst, test_name, 'inout')
        finalout = os.path.join(self.burst, test_name, 'out')
        assert(os.path.exists(intfile))
        assert(os.path.exists(stagein1))
        assert(os.path.exists(stagein2))
        assert(not os.path.exists(files[2]))
        # if the vdo is not added to a vds, then madats does not manage the data,
        # i.e., the data will not be copied/moved to different storage tier;
        # however, the associated task will still be executed but with the default
        # datapath
        assert(not os.path.exists(finalout))
        output = self.__get_file_data__(files[3])
        input = ''.join(strdata)
        assert("{}".format(input) == output)


    '''
    TEST-15: Manage a complex workflow with multiple input output dependecies
    '''
    def test_complex_workflow(self):
        test_name = 'test_complex_workflow'
        datadir = os.path.join(self.scratch, test_name)
        if not os.path.exists(datadir):
            os.makedirs(datadir)
        strdata = []
        files = [os.path.join(datadir, 'in1'), os.path.join(datadir, 'in2'),
                 os.path.join(datadir, 'in3'), os.path.join(datadir, 'in4'),
                 os.path.join(datadir, 'inout1'), os.path.join(datadir, 'inout2'),
                 os.path.join(datadir, 'out')]
        for i in range(len(files) - 3):
            strdata.append(self.__get_random_string__())
            self.__create_file__(files[i], strdata[i])

        # create a VDS
        vds = madats.VirtualDataSpace()
        vds.strategy = madats.Policy.STORAGE_AWARE
        
        # create VDOs
        vdo1 = madats.VirtualDataObject(files[0])
        vdo2 = madats.VirtualDataObject(files[1])
        vdo3 = madats.VirtualDataObject(files[2])
        vdo4 = madats.VirtualDataObject(files[3])
        vdo5 = madats.VirtualDataObject(files[4])
        vdo6 = madats.VirtualDataObject(files[5])
        vdo7 = madats.VirtualDataObject(files[6])

        # create tasks
        task1 = madats.Task(command='cat')
        task1.params = [vdo1, vdo2, '>', vdo5]
        task2 = madats.Task(command='echo')
        str1 = self.__get_random_string__()
        task2.params = [str1, '>', vdo3]
        task3 = madats.Task(command='echo')
        str2 = self.__get_random_string__()
        task3.params = [str2, '>', vdo4]
        task4 = madats.Task(command='cat')
        task4.params = [vdo1, vdo3, vdo5, '>', vdo6]
        task5 = madats.Task(command='cat')
        task5.params = [vdo2, vdo4, vdo6, '>', vdo7]
    
        # define VDO and task associations
        vdo1.consumers = [task1, task4]
        vdo2.consumers = [task1, task5]
        vdo3.producers = [task2]
        vdo3.consumers = [task4]
        vdo4.producers = [task3]
        vdo4.consumers = [task5]
        vdo5.producers = [task1]
        vdo5.consumers = [task4]
        vdo6.producers = [task4]
        vdo6.consumers = [task5]
        vdo7.producers = [task5]
        vds.add(vdo1)
        vds.add(vdo2)
        vds.add(vdo3)
        vds.add(vdo4)
        vds.add(vdo5)
        vds.add(vdo6)
        vds.add(vdo7)
        
        # manage VDS
        madats.manage(vds)

        stagein1 = os.path.join(self.burst, test_name, 'in1')
        stagein2 = os.path.join(self.burst, test_name, 'in2')
        stagein3 = os.path.join(self.burst, test_name, 'in3')
        stagein4 = os.path.join(self.burst, test_name, 'in4')
        intfile1 = os.path.join(self.burst, test_name, 'inout1')
        intfile2 = os.path.join(self.burst, test_name, 'inout2')
        finalout = os.path.join(self.burst, test_name, 'out')
        # since it's storage-aware, it creates the intermediate file and stages in all the
        # inputs on fast burst tier
        # there is no final output of the workflow as the second task only takes i/p and no o/p
        assert(os.path.exists(stagein1))
        assert(os.path.exists(stagein2))
        assert(os.path.exists(stagein3))
        assert(os.path.exists(stagein4))
        assert(os.path.exists(intfile1))
        assert(os.path.exists(intfile2))
        assert(not os.path.exists(files[4]))
        assert(not os.path.exists(files[5]))
        assert(os.path.exists(finalout))
        assert(os.path.exists(files[6]))
        output = self.__get_file_data__(files[6])
        input_strs = [strdata[1], str2, '\n', strdata[0], str1, '\n', strdata[0], strdata[1]]
        input = ''.join(input_strs)
        #print(input_strs, output)
        assert("{}".format(input) == output)

