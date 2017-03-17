from abstractions.system_interfaces import AbstractCopy

class MadatsCopy(CopyAbstract):
    def copy(self, src, dest, async=False, keep_src=True, **kwargs):
        pass


    def poll(self, id, **kwargs):
        pass
