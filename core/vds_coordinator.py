"""
manages a virtual data space (VDS) for a workflow
"""

from vds import VirtualDataSpace

'''
uses data management strategies, creates data tasks and a DAG
-- decides `WHO' moves the data
'''
class DataManager():
    STORAGE = 'STORAGE'
    MIDDLEWARE = 'MIDDLEWARE'
    SELECTIVE = 'SELECTIVE'

    STORAGE_MANAGED = 0  # data abstraction = UNIFIED : all data has single unified namespace or need not be managed by VDS
    VDS_MANAGED = 1      # data abstraction = HIERARCHICAL : all data has specific storage id -- three strategies (STA, WFA, PAS)
    SELECTIVE_MANAGED = 2   # data abstraction = HYBRID : some use unified namespace and some specific; storage managed namespace needs to be specified; for specific either user or workflow specifies


    def __init__(self, strategy):
        self.strategy = strategy
        self.vds = VirtualDataSpace()
        self.data_manager = None

        if strategy == STORAGE_MANAGED:
            self.data_manager = STORAGE
        elif strategy == VDS_MANAGED:
            self.data_manager = MIDDLEWARE
        elif strategy == SELECTIVE_MANAGED:
            self.data_manager = SELECTIVE
        else:
            print("Invalid data manager specified")
            sys.exit(-1)


    def create_data_task(self, vdo_src, dest_fs_id):
        vdo_dest = vdo_src.copy(dest_fs_id)

        # if staging in
        if len(vdo_src.producers) == 0:
            data_task = DataTask(vdo_src, vdo_dest)
            vdo_dest.producers = [data_task]
            vdo_src.consumers = [data_task]
        else:
            vdo_dest.producers = [prod for prod in vdo_src.producers]

        # if staging out
        if len(vdo_src.consumers) == 0:
            data_task = DataTask(vdo_src, vdo_dest)
            vdo_dest.consumers = [data_task]
            vdo_src.producers = [data_task]
        else:
            vdo_dest.consumers = [cons for cons in vdo_src.consumers]


    def create_dag(self):
        dag = {}
        for vdo_name in self.vds.vdos:
            vdo = self.vdo_dict[vdo_name]
            for prod in vdo.producers:
                if prod not in dag:
                    dag[prod] = []
                for cons in vdo.consumers:
                    if cons not in dag[prod]:
                        dag[prod].append(cons)

        return dag
    
######################################################################################
"""
defines data management strategies for VDS_MANAGED workflow
-- decides `WHERE' to move the data
"""
class DataManagementStrategy():

    USER_DRIVEN = 0
    STORAGE_AWARE = 1
    WORKFLOW_AWARE = 2

    def __init__(self, data_management):
        self.data_management = data_management
        if data_management == USER_DRIVEN:
            self.storage_managed()
        elif data_management == STORAGE_AWARE:
            self.vds_managed()
        elif data_management == WORKFLOW_AWARE:
            self.selective_managed()
        else:
            print("Invalid data management strategy selected")
            sys.exit(-1)

    def user_driven(self):
        return

    def storage_aware(self):
        return

    def workflow_aware(self):
        return
    
######################################################################################

'''
manages the DAG of a workflow containing compute and data tasks
-- decides `WHEN' to move the data
'''
class DAGManager():

    DEADLINE_BASED = 0 # deadlines for each data task needs to be calculated
    JUST_IN_TIME = 1   # each data task is executed just-in-time
    SPECULATIVE = 2

    def __init__(self, strategy):
        self.strategy = strategy
        if strategy == DEADLINE_BASED:
            self.deadline_based()
        elif strategy == JUST_IN_TIME:
            self.just_in_time()
        elif strategy == SPECULATIVE:
            self.speculative()
        else:
            print("Invalid DAG management strategy")
            sys.exit(-1)

    def deadline_based(self):
        return

    def just_in_time(self):
        return

    def speculative(self):
        return
