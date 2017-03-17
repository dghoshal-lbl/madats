from abstractions.system_interfaces import AbstractCopy

class MadatsCopy(AbstractCopy):
    def copy(self, src, dest, async=False, keep_src=True, **kwargs):
        pass


    def poll(self, id, **kwargs):
        pass


if __name__ == '__main__':
    cp = MadatsCopy()
    cp.copy('src', 'dst')
    cp.poll(0)
