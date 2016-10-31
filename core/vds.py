"""
module to create a Virtual Data Space
"""

# make this a generic interface to multiple file systems
import filesystem_interface as fsi

__VDS_MOUNTPT__ = '/vds'

'''
creates and operates on a virtual data object
'''
class VirtualDataObject():
    def __init__(self, vds, fs_id, fs_object):
        self.fs_id = fs_id
        self.fs_object = fs_object
        self.vdo_name = self.__create_vdo__()
        self.copiedTo = []  # can be copied to multiple storage layers at a time
        self.copiedFrom = None  # can be copied from only one storage layer at a time
        self.producers = []
        self.consumers = []
        self.persist = True
        vds.add(self)

    def __create_vdo__(self):
        mount_pt = fsi.get_fs_map(self.fs_id)
        vds_path = self.fs_object.replace(mount_pt, __VDS_MOUNTPT__)
        return (self.fs_id + ':' + vds_path)
        
    def copy(self, dest_fs_id):
        fs_object = self.fs_object
        vdo = VirtualDataObject(dest_fs_id, fs_object)
        self.copiedTo.append(dest_fs_id)
        vdo.copiedFrom = self.fs_id
        return vdo

    def producer(self, vdo, task):
        self.producers.append(task)

    def consumer(self, vdo, task):
        self.consumers.append(task)

    def remove(self, vdo):
        self.persist = False

######################################################################################

'''
creates a virtual data space connecting the virtual data objects through
other virtual data objects and tasks
'''
class VirtualDataSpace():
    def __init_(self):
        self.vdo_dict = {}
        self.vdos = []

    def add(self, vdo):
        self.vdo_dict[vdo.vdo_name] = vdo
        self.vdos.append(vdo.vdo_name)
