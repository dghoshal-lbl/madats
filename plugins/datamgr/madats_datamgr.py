from abstractions.data_interfaces import AbstractDatamgr 

class MadatsDatamgr(AbstractDatamgr):
    def policy_engine(self, vds, policy, **kwargs):
        if policy == 'WFA':
            self.__madats_wfa__(vds)
        elif policy == 'STA':
            self.__madats_sta__(vds)
        else:
            self.__default__(vds)


    def __madats_wfa__(self, vds):
        # TODO implementation
        return 0
            

    def __madats_sta__(self, vds):
        # TODO implementation
        return 0
            

    def __default__(self, vds):
        vdos = vds.get_vdo_list()
        for vdo in vdos:
            new_vdo = vds.copy(vdo, 'burst')
            self.replace_vdo(vds, vdo, new_vdo)
        return 0
            

    # optimal madats: new data management strategy
    '''
    - combine (viable) data movements together into a single data task
    - priority-based data movment: data on which most tasks depend are moved first
    - use specific data movement plugins: data transfer nodes, datawarp API
    '''
    def __madats_opt__(self, vds):
        vdos = vds.get_vdo_list()
        
