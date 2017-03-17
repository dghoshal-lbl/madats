from abstractions.data_interfaces import AbstractDatamgr 

class MadatsDatamgr(AbstractDatamgr):
    def policy(self, vds, **kwargs):
        strategy = None
        if 'strategy' in kwargs:
            strategy = kwargs['strategy']

        if strategy == 'WFA':
            self.__madats_wfa__(vds)
        elif strategy == 'STA':
            self.__madats_sta__(vds)
        else:
            self.__default__(vds)


    def __madats_wfa__(self, vds):
        # TODO implementation
        pass
            

    def __madats_sta__(self, vds):
        # TODO implementation
        pass

            
    def __default__(self, vds):
        vdos = vds.get_vdo_list()
        for vdo in vdos:
            new_vdo = vds.copy(vdo, 'burst')
            self.create_data_task(vds, vdo, new_vdo)
            
            
