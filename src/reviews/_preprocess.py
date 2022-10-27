import os
import re
import sys
import time
import pickle
import unicodedata
from datetime import datetime
from tqdm.auto import tqdm
import numpy as np
import pandas as pd

# Exception Error Handling
import warnings
warnings.filterwarnings("ignore")

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src = os.path.abspath(os.path.join(cur_dir, os.pardir))
    sys.path.append(src)

tbl_cache = os.path.join(root, 'tbl_cache')
conn_path = os.path.join(root, 'conn.txt')

# import module inside package
from access_database.access_db import AccessDataBase
from mapping._preprocessing import TitlePreProcess

class ReviewMapping:
    def __init__(self):
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = AccessDataBase(conn[0], conn[1], conn[2])
        
        # title preprocess
        self.tp = TitlePreProcess()
        
        # path
        self.name = 'reviews_upload.csv'
        self.path = os.path.join(tbl_cache, self.name)
        
        # day
        self.today = datetime.today().strftime('%Y-%m-%d')
        
    def select(self, category):
        ''' Select Table '''
        
        map_df = self.db.get_tbl('beauty_kr_mapping_table')
        map_df.loc[:, 'source'] = map_df.source + '_review'        
        
        gl_info = self.db.get_tbl('glowpick_product_info_final_version', ['id', 'selection', 'division', 'groups'])
        gl_info.loc[:, 'table_name'] = 'glowpick_product_info_final_version'
        gl_info = self.tp.categ_reclassifier(gl_info)
        gl_info_categ = gl_info.loc[gl_info.category==category].reset_index(drop=True)
        
        # extracting item keys
        item_keys = gl_info_categ.id.tolist()

        # grouping by source (table)
        groups = map_df.loc[map_df.item_key.isin(item_keys), ['item_key', 'mapped_id', 'source']].groupby('source')
        indices = groups.indices

        # select table 
        db_table = self.db.get_tbl_name()
        df_list = []
        conn, curs = self.db.db_connect()
        for table in tqdm(indices.keys()):
            ids = tuple(groups.get_group(table).mapped_id.tolist())
            
            if table in db_table:
                query = f'SELECT `id`, `user_id`, `product_rating`, `review_date`, `product_review` FROM `{table}` WHERE `id` IN {ids}'
                curs.execute(query)
                tbl = curs.fetchall()
                df = pd.DataFrame(tbl)
                df.loc[:, 'source'] = table
                df_list.append(df)
            else:
                pass
        
        # concat & mapping (convert id -> item_key)
        reviews = pd.concat(df_list).reset_index(drop=True)
        reviews_merge = reviews.merge(map_df, left_on=['id', 'source'], right_on=['mapped_id', 'source'], how='left')
        
        # select glowpick data
        table = 'glowpick_product_info_final_version_review'
        ids = tuple(item_keys)
        query = f'SELECT `id`, `user_id`, `product_rating`, `review_date`, `product_review` FROM `{table}` WHERE `id` IN {ids}'
        curs.execute(query)
        tbl = curs.fetchall()
        df = pd.DataFrame(tbl)
        df.loc[:, 'source'] = table
        df = df.rename(columns={'id': 'item_key'})
        
        curs.close()
        conn.close()
        
        # concat
        self.reviews = pd.concat([reviews_merge, df]).loc[:, ['item_key', 'user_id', 'product_rating', 'review_date', 'product_review', 'source']]
        
    def dup_check(self):
        '''  Preprocessing & Duplicate check '''

        # remove null values
        reviews_data = self.reviews[self.reviews.product_review.notnull()].reset_index(drop=True)
        
        # encoding
        reviews_data.loc[:, 'encoded'] = reviews_data.product_review.apply(lambda x: unicodedata.normalize('NFC', x))

        # regulization
        reg = re.compile(r'[^가-힣a-zA-Z0-9 ]')
        reviews_data.loc[:, 'preprocessed'] = reviews_data.encoded.str.replace(reg, '').str.replace(r' +', ' ').str.strip()

        # Remove missing values
        reviews_data.loc[reviews_data.preprocessed=='', 'preprocessed'] = np.nan
        reviews_data_notnull = reviews_data[reviews_data.preprocessed.notnull()].reset_index(drop=True)
        
        # duplicate check
        subset = ['item_key', 'user_id', 'preprocessed']
        self.reviews_data_dedup = reviews_data_notnull.drop_duplicates(subset=subset, keep='first')
            
    def create(self):
        ''' Create table '''
        
        # sorting & convert object to datetime
        reviews_sorted = self.reviews_data_dedup.sort_values(by='item_key').reset_index(drop=True)
        reviews_sorted.loc[:, 'review_date'] = pd.to_datetime(reviews_sorted.review_date, errors='coerce')
        
        # columns rename
        rename = {
            'id': 'item_key',
            'review_date': 'write_date',
            'preprocessed': 'txt_data',
        }
        columns = ['item_key', 'txt_data', 'write_date', 'product_rating', 'source']
        self.reviews_upload = reviews_sorted.rename(columns=rename).loc[:, columns]

        # regist_date
        today = datetime.today().strftime('%Y-%m-%d')
        self.reviews_upload.loc[:, 'regist_date'] = pd.Timestamp(today)
        
        # save file
        self.reviews_upload.to_csv(self.path, index=False)
        
        return self.reviews_upload