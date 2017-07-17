from plugins import plugin_loader
from madats.vds import VirtualDataObject
import sys

'''
madats interface to invoke the copy plugin
- this is where the async property should be set to enable asynchronous data transfer
'''
def main():
    if len(sys.argv) < 3:
        print("Usage: {} <src> <dest>".format(sys.argv[0]))
        sys.exit(-1)


    src = sys.argv[1]
    dest = sys.argv[2]
    vdo_src = VirtualDataObject(src)
    vdo_dest = VirtualDataObject(dest)

    copy_plugin = plugin_loader.load_copy_plugin()
    copy_plugin.copy(vdo_src, vdo_dest)


if __name__ == '__main__':
    main()

    
