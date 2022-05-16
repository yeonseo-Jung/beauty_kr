import os
import re
import sys
import pickle
from datetime import datetime
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
from access_database import access_db
from mapping import preprocessing, mapping_product


class ReviewMapping:
    def __init__(self):
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = access_db.AccessDataBase(conn[0], conn[1], conn[2])
        
    def get_table(self, reviews):
        
        # 매핑테이블
        map_tbl = self.db.get_tbl('naver_glowpick_mapping_table', 'all')

        info_columns = ['id', 'selection', 'division', 'groups']
        review_columns = ['id', 'review_date', 'user_id', 'product_review']

        # 매핑 기준 테이블
        infos_0 = ['glowpick_product_info_final_version']
        reviews_0 = ['glowpick_product_info_final_version_review']
        info_0 = self.db.integ_tbl(infos_0, info_columns)
        review_0 = self.db.integ_tbl(reviews_0, review_columns)
        
        # 매핑 대상 테이블
        review_1 = self.db.integ_tbl(reviews, review_columns)
        
        return map_tbl, info_0, review_0, review_1
    
    def _mapping(self, map_tbl, review_0, review_1):
        # 매핑 대상 리뷰 매핑
        map_tbl_ = mapping_product.map_expand(map_tbl)
        map_tbl_.loc[:, 'table_name'] = map_tbl_.table_name + "_review"
        review_1_mapped = map_tbl_.merge(review_1, left_on=['id', 'table_name'], right_on=['id', 'table_name'], how='inner').drop(columns='id')

        review_0_mapped = review_0.rename(columns={'id': 'item_key'})
        
        return review_0_mapped, review_1_mapped
    
    def _integ(self, info_0, review_0_mapped, review_1_mapped):
        # category integrated
        prepro = preprocessing.TitlePreProcess()
        info_0_categ = prepro.categ_reclassifier(info_0, 0)
        info_0_categ_ = info_0_categ[info_0_categ.category.notnull()].loc[:, ['id', 'table_name', 'category']].reset_index(drop=True)

        # concat review table
        review_concat = pd.concat([review_0_mapped, review_1_mapped]).reset_index(drop=True)

        # merge & rename
        rev_info = review_concat.merge(info_0_categ_.loc[:, ['id', 'category']], left_on='item_key', right_on='id', how='inner').drop(columns='id')
        rev_info_ = rev_info.rename(columns={'product_review': 'txt_data', 'review_date': 'write_date', 'user_id': 'user_info', 'table_name': 'source'})
        
        return rev_info_

    def dup_check(self, rev_info):
        ''' 동일 상품 리뷰 중복 제거 '''
        
        dup_columns = ['user_info', 'write_date', 'txt_data']
        # dedup & sorting 
        rev_info = rev_info[rev_info.txt_data.notnull()]
        rev_info_dedup = rev_info.drop_duplicates(subset=dup_columns, keep='first').sort_values(by=['item_key', 'source']).reset_index(drop=True)
        return rev_info_dedup
    
    def upload_review_table(self, rev_info_dedup):
        ''' 리뷰 테이블 업로드 형식으로 수정 '''
        
        # columns sorting & drop null
        rev_info_dedup.loc[:, 'write_date'] = rev_info_dedup.write_date.astype('str')
        rev_info_dedup.loc[:, 'write_date'] = rev_info_dedup.write_date.str.replace('.', '-')
        reg_date = re.compile('[0-9]+[-]+[0-9]+[-]+[0-9]+')
        idx = rev_info_dedup.index.tolist()
        _idx = rev_info_dedup[rev_info_dedup.write_date.str.fullmatch(reg_date, na=False)].index.tolist()
        df_idx = pd.DataFrame(idx + _idx, columns=['idx'])
        idx_ = df_idx.drop_duplicates('idx', keep=False).idx.tolist()
        rev_info_dedup.loc[idx_, 'write_date'] = np.nan
        rev_info_dedup = rev_info_dedup[rev_info_dedup.txt_data.notnull()]
        
        year = str(datetime.today().year)
        month = str(datetime.today().month)
        day = str(datetime.today().day)
        if len(month) == 1:
            month = "0" + month
        if len(day) == 1:
            day = "0" + day
        date = year + "-" + month + "-" + day
        
        rev_info_dedup.loc[:, 'regist_date'] = date
        rev_info_dedup.loc[:, 'pk'] = range(len(rev_info_dedup))
        columns = ['pk', 'item_key', 'txt_data', 'write_date', 'regist_date', 'source']
        
        upload_df = rev_info_dedup.loc[:, columns]
        return upload_df