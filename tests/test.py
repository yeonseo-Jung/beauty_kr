import os
import sys
import pymysql
import pandas as pd

cur_dir = os.path.dirname(os.path.realpath(__file__))
root = os.path.abspath(os.path.join(cur_dir, os.pardir))
backup = root + '/backup'
cache = root + '/tbl_cache'

''' test access_database '''
from access_database import access_db
user_name = 'yeonseosla'
password = 'jys9807'
database = 'beauty_kr'



# table_name = ''
# columns = ['id', 'brand_name', 'product_name', 'selection', 'division', 'groups']

db = access_db.AccessDataBase(user_name, password, database)

# # get mapping table
# db.get_tbl('naver_glowpick_mapping_table', 'all').to_csv(cache + '/mapping_table.csv', index=False)




''' test preprocess & reclassify '''
from mapping import preprocessing

# title preprocessing & category reclassify
# df_0 = preprocessing.title_preprocessor(tbl_0.reset_index(drop=True))
# df_categ_0 = preprocessing.reclassifier(tbl_0, 0)
# df_deprepro_0 = df_0.merge(df_categ_0, on='id', how='left').sort_values('id').reset_index(drop=True)
# df_deprepro_0.to_csv(f'df_deprepro_{table_name_0}.csv', index=False)

# df_1 = preprocessing.title_preprocessor(tbl_1)
# df_categ_1 = preprocessing.reclassifier(tbl_1, 1)
# df_deprepro_1 = df_1.merge(df_categ_1, on='id', how='left').sort_values('id').reset_index(drop=True)
# df_deprepro_1.to_csv(f'df_deprepro_{table_name_1}.csv', index=False)


''' test mapping '''
from mapping import mapping_product

#
# df_deprepro_0 = pd.read_csv('df_deprepro_0.csv')

# # compare product name & mapping
# compared_df = mapping_product.mapping_prd(df_deprepro_0, df_deprepro_1)
# compared_df.to_csv(f'df_compared_{table_name_1}.csv', index=False)

# # select mapped product
# mapped_df = mapping_product.select_mapped_prd(compared_df)
# mapped_df.to_csv(f'df_mapped_{table_name_1}.csv', index=False)

# return mapping table
# mapping_table = mapping_product.md_map_tbl(mapped_df, table_name_1)
# mapped_df = pd.read_csv('backup/df_mapped_naver_beauty_product_info_extended_v4_220311.csv')
# mapping_table = mapping_product.md_map_tbl(mapped_df, table_name_1)
# mapping_table.to_csv(f'mapping_table_{table_name_1}.csv', index=False)

# mapping_table = mapping_product.concat_map_tbl()

# outputs = mapping_product.update_map_tbl(user_name, password, db_name)
# print(f'\n\t {outputs[0]} \n\t - mapped glowpick product counts: {outputs[1]} \n\t - mapped naver product counts: {outputs[2]}')

# table_name = 'naver_glowpick_mapping_table_test'
# columns = ['glowpick_product_info_final_version_id', 'mapped_id', 'table_name']
# outputs = mapping_product.upload_map_tbl(user_name, password, db_name, table_name, columns)
# print(outputs)


''' GUI '''
from gui import gui_main
from PyQt5.QtWidgets import *

gui_main.exec_gui()