import os
import re
import sys
import pandas as pd

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
    form_dir = os.path.join(root, 'form')
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src = os.path.abspath(os.path.join(cur_dir, os.pardir))
    sys.path.append(src)
    
from access_database.access_db import AccessDataBase
from mapping._preprocessing import TitlePreProcess
tp = TitlePreProcess()

user_name = "yeonseosla"
password = "jys9807"
db_name = "beauty_kr"
db = AccessDataBase(user_name, password, db_name)

''' Update Data for Dashboard '''

category_list = [
    'skin_care',
    'makeup',
    'body_care',
    'hair_care',
    'sun_care',
    'cleansing',
    'mens_care',
    'mask_pack',
    'beauty_tool',
    'fragrance',
]

if __name__ == 'main':
    # Maaping Status
    mapping_table = db.get_tbl('beauty_kr_mapping_table')
    gl_info = db.integ_tbl(['glowpick_product_info_final_version'], ['id', 'product_code', 'brand_name', 'selection', 'division', 'groups', 'dup_check'])
    gl_info_categ = tp.categ_reclassifier(gl_info)
    gl_info_dedup = gl_info_categ[gl_info_categ.dup_check!=-1].rename(columns={'id':'item_key'}).reset_index(drop=True)
    item_keys_mapping = mapping_table.item_key.unique()

    # Available Status
    info_all = re.compile(r'beauty_kr_[a-z\_]+_info_all')
    reviews_all = re.compile(r'beauty_kr_[a-z\_]+_reviews_all')
    tables = db.get_tbl_name()

    info_tables, review_tables = [], []
    for table in tables:
        matched_info = re.search(info_all, table)
        matched_review = re.search(reviews_all, table)
        if matched_info == None:
            pass
        else:
            info_tables.append(matched_info.group(0))
            
        if matched_review == None:
            pass
        else:
            review_tables.append(matched_review.group(0))
            
    info_tables = set(info_tables)
    review_tables = set(review_tables)

    # select table
    info_all_df = db.integ_tbl(info_tables, ['item_key', 'product_status'])
    item_keys_available = info_all_df.item_key.unique()

    review_all_df = db.integ_tbl(review_tables, ['item_key', 'product_rating'])
    cnt_df = review_all_df.groupby('item_key').count()
    review_count_df = pd.DataFrame(cnt_df.product_rating).rename(columns={'product_rating':'review_count'})

    columns = ['item_key', 'brand_name', 'category', 'mapping_status', 'review_count', 'available_status']
    dashboard_df = gl_info_dedup.loc[:, ['item_key', 'brand_name', 'category']]

    # mapping
    dashboard_df.loc[dashboard_df.item_key.isin(item_keys_mapping), 'mapping_status'] = True
    dashboard_df.loc[dashboard_df.item_key.isin(item_keys_mapping)==False, 'mapping_status'] = False

    # available
    dashboard_df.loc[dashboard_df.item_key.isin(item_keys_available), 'available_status'] = True
    dashboard_df.loc[dashboard_df.item_key.isin(item_keys_available)==False, 'available_status'] = False

    # review count
    dashboard_df_merge = dashboard_df.merge(review_count_df, left_on='item_key', right_on=review_count_df.index, how='left')
    
    # create table
    # db.create_table(dashboard_df_merge, 'beauty_kr_data_dashboard')
    print(dashboard_df_merge.tail())