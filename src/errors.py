import os
import sys
import time
import pickle
import traceback
from datetime import datetime
import pandas as pd
from access_database.access_db import AccessDataBase

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir))

tbl_cache = os.path.join(root, 'tbl_cache')
conn_path = os.path.join(root, 'conn.txt')

class Errors:
    def __init__(self):
        self.init_db()
    
    def init_db(self):
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = AccessDataBase(conn[0], conn[1], conn[2])
        
    def errors_log(self, url=None):
        tb = traceback.format_exc()
        _datetime = pd.Timestamp(datetime.today())
        table = 'error_log'
        fields = ('url', 'traceback', 'error_date')
        values = (url, tb, _datetime)
        
        while True:
            try:
                self.db.insert(table, fields, values)
                break
            except Exception as e:
                print(e)
                time.sleep(100)
                self.init_db()