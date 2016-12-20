import sys
from db.loader import DbLoader

if __name__ == '__main__':
    db_loader = DbLoader(collection='tasks')
    task_id = sys.argv[1]
    print(task_id)
    db_loader.update_status(task_id, 'COMPLETED')
