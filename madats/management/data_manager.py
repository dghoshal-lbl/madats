from madats.utils.constants import Policy

def dm_workflow_aware(vds):
    pass


def dm_storage_aware(vds):
    pass

def plan(vds, policy):
    if policy == Policy.NONE:
        return
    elif policy == Policy.WORKFLOW_AWARE:
        dm_workflow_aware(vds)
    elif policy == Policy.STORAGE_AWARE:
        dm_storage_aware(vds)
