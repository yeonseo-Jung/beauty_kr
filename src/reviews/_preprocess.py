import os
import re
import sys
import time
import pickle
from datetime import datetime
from tqdm.auto import tqdm
import numpy as np
import pandas as pd


# Exception Error Handling
import warnings
warnings.filterwarnings("ignore")

# current directory
cur_dir = os.path.dirname(os.path.realpath(__file__))

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    conn_path = os.path.join(base_path, 'conn.txt')
    
else:
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src = os.path.abspath(os.path.join(cur_dir, os.pardir))
    sys.path.append(root)
    sys.path.append(src)
    tbl_cache = os.path.join(root, 'tbl_cache')
    conn_path = os.path.join(src, 'gui', 'conn.txt')

# import module inside package
from access_database.access_db import AccessDataBase

class ReviewMapping:
    def __init__(self):
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = AccessDataBase(conn[0], conn[1], conn[2])
        
        # path
        self.name = 'reviews_upload.csv'
        self.path = os.path.join(tbl_cache, self.name)
        
        # day
        today = datetime.today()
        year = str(today.year)
        month = str(today.month)
        day = str(today.day)
        if len(month) == 1:
            month = "0" + month
        if len(day) == 1:
            day = "0" + day
        self.date = year + "-" + month + "-" + day
        
    def select(self, category):
        ''' Select Table '''
        
        map_df = self.db.get_tbl('beauty_kr_mapping_table')
        categ_info = self.db.get_tbl(f'beauty_kr_{category}_info_all', ['item_key'])        
        
        # extracting item keys
        item_keys = categ_info.item_key.tolist()

        # grouping by source (table)
        groups = map_df.loc[map_df.item_key.isin(item_keys), ['mapped_id', 'source']].groupby('source')
        indices = groups.indices
        indices['glowpick_product_info_final_version'] = item_keys

        # select table 
        db_table = self.db.get_tbl_name()
        df_list = []
        for table in tqdm(indices.keys()):
            ids = tuple(indices[table])
            table = table + '_review'
            
            if table in db_table:
                query = f'SELECT `id`, `user_id`, `product_rating`, `review_date`, `product_review` FROM `{table}` WHERE `id` IN {ids}'
                curs = self.db.db_connect()
                curs.execute(query)
                tbl = curs.fetchall()
                df = pd.DataFrame(tbl)
                df.loc[:, 'source'] = table
                df_list.append(df)
            else:
                pass

        curs.close()
        self.reviews = pd.concat(df_list).reset_index(drop=True)
        
    def dup_check(self):
        ''' Duplicate check '''
        
        # duplicate check
        reviews_notnull = self.reviews[self.reviews.product_review.notnull()]
        subset = ['user_id', 'review_date', 'product_review']
        self.reviews_dedup = reviews_notnull.drop_duplicates(subset=subset, keep='first').reset_index(drop=True)
    
    def create(self):
        ''' Create table '''
        
        # convert object to datetime
        reviews_sorted = self.reviews_dedup.sort_values(by='id').reset_index(drop=True)
        reviews_sorted.loc[:, 'review_date'] = pd.to_datetime(reviews_sorted.review_date, errors='coerce')
        
        # columns rename
        rename = {
            'id': 'item_key',
            'review_date': 'write_date',
            'product_review': 'txt_data',
        }
        columns = ['item_key', 'txt_data', 'write_date', 'source']
        self.reviews_upload = reviews_sorted.rename(columns=rename).loc[:, columns]

        # regist_date
        today = datetime.today()
        y = today.year
        m = today.month
        d = today.day
        self.reviews_upload.loc[:, 'regist_date'] = pd.Timestamp(f'{y}-{m}-{d}')
        
        # save file
        self.reviews_upload.to_csv(self.path, index=False)