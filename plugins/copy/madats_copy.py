from abstractions.system_interfaces import AbstractCopy
from utils import helper
import os

class MadatsCopy(AbstractCopy):
    '''
    def copy(self, vdo_src, vdo_dest, async=False, keep_src=True, **kwargs):
        print('copying {} to {}'.format(vdo_src.abspath, vdo_dest.abspath))
        if vdo_src.storage_id == 'archive':
            command1 = 'htar -xf ' + vdo_src.abspath
            self.run(command1)
            filename = os.path.basename(vdo_src.abspath)
            untar_file = os.path.splitext(filename)[0]
            command2 = 'mv ' + untar_file + ' ' + vdo_dest.abspath
            self.run(command2)
        elif vdo_dest.storage_id == 'archive':
            curdir = os.getcwd()
            datadir = os.dirname(vdo_src.abspath)
            filename = os.path.basename(vdo_src.abspath)
            os.chdir(datadir)
            command = 'htar -cf ' + vdo_dest.relative_path + ' ' + filename
            self.run(command)
            os.chdir(curdir)
        else:
            command = 'cp ' + vdo_src.abspath + ' ' + vdo_dest.abspath
            self.run(command)
    '''
    def copy(self, vdo_src, vdo_dest, async=False, keep_src=True, **kwargs):
        print('copying {} to {}'.format(vdo_src.abspath, vdo_dest.abspath))
        if vdo_src.storage_id == 'archive':
            dest_dir = os.path.dirname(vdo_dest.abspath)
            command = 'mkdir -p {}; cd {}; hsi -q "prompt; mget {}"'.format(dest_dir, vdo_dest.abspath, vdo_src.abspath)
        elif vdo_dest.storage_id == 'archive':
            src_dir = os.path.dirname(vdo_src.abspath)
            dest_dir = os.path.dirname(vdo_dest.abspath)       
            filename = os.path.basename(vdo_dest.abspath)
            command = 'cd {}; hsi -q "prompt; mkdir -p {}; cd {}; mput {}"'.format(src_dir, dest_dir, dest_dir, filename)
        else:
            command = 'cp ' + vdo_src.abspath + ' ' + vdo_dest.abspath
        helper.run(command)


    def poll(self, id, **kwargs):
        pass


if __name__ == '__main__':
    cp = MadatsCopy()
    cp.copy('src', 'dst')
    cp.poll(0)
