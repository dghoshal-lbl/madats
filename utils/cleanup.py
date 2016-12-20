import sys
from db.loader import DbLoader

if __name__ == '__main__':
    db_loader = DbLoader()
    db_loader.truncate(['workflows', 'tasks', 'data_tasks'])
