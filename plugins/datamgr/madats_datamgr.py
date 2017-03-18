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
            self.create_data_task(vds, vdo, new_vdo)
        return 0
            
