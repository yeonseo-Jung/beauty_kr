import os
import re
import sys
from tqdm.auto import tqdm

import numpy as np
import pandas as pd

cur_dir = os.path.dirname(os.path.realpath(__file__))
root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
src = os.path.abspath(os.path.join(cur_dir, os.pardir))
sys.path.append(root)
sys.path.append(src)

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    
else:
    base_path = os.path.dirname(os.path.realpath(__file__))
    tbl_cache = os.path.join(root, 'tbl_cache')

from PyQt5 import QtCore
from mapping._preprocessing import *

preprocessor = TitlePreProcess()
class ThreadTitlePreprocess(QtCore.QThread, QtCore.QObject):
    
    ''' Thread preprocessing product name '''
    
    def __init__(self, parent=None):
        super().__init__()
        self.preprocess = TitlePreProcess()
        self.power = False
        
        # file path
        self.tbl_0 = os.path.join(tbl_cache, 'tbl_0.csv')
        self.tbl_1 = os.path.join(tbl_cache, 'tbl_1.csv')
        self.tbl_deprepro = os.path.join(tbl_cache, 'tbl_deprepro.csv')
        
    def _categ_reclaasify(self):
        df_0 = pd.read_csv(self.tbl_0)
        df_1 = pd.read_csv(self.tbl_1)
        df_dup = grouping(df_0)
        df_dedup = df_dup.loc[df_dup.dup_check != -1]
        df_0_dedup = df_0.merge(df_dedup, on='id', how='inner')
        
        self.df_0_categ = preprocessor.categ_reclassifier(df_0_dedup, 0)
        self.df_1_categ = preprocessor.categ_reclassifier(df_1, 1)
        
    progress = QtCore.pyqtSignal(object)
    def run(self):
        
        t = tqdm(range(len(self.df_0_categ) + len(self.df_1_categ)))
        idx = 0
        for i in t:        
            if self.power:
                self.progress.emit(t)
                
                if idx >= len(self.df_0_categ):
                    idx_ = idx - len(self.df_0_categ) 
                    title = self.df_1_categ.loc[idx_, 'product_name']
                    brand = self.df_1_categ.loc[idx_, 'brand_name']
                    title_, keep_wd_dict  = preprocessor.title_preprocessor(title, brand)
                    self.df_1_categ.loc[idx_, 'product_name'] = str(title_)
                    if len(keep_wd_dict) == 0:
                        self.df_1_categ.loc[idx_, 'keep_words'] = np.nan
                    else:
                        self.df_1_categ.loc[idx_, 'keep_words'] = str(keep_wd_dict)
                
                else:
                    title = self.df_0_categ.loc[idx, 'product_name']
                    brand = self.df_0_categ.loc[idx, 'brand_name']
                    title_, keep_wd_dict = preprocessor.title_preprocessor(title, brand)
                    self.df_0_categ.loc[idx, 'product_name'] = str(title_)
                    if len(keep_wd_dict) == 0:
                        self.df_0_categ.loc[idx, 'keep_words'] = np.nan
                    else:
                        self.df_0_categ.loc[idx, 'keep_words'] = str(keep_wd_dict)
                idx += 1
            else:
                break
            
            self.progress.emit(t)
            self.poewr = False
            
        columns = ['id', 'brand_name', 'product_name', 'category', 'keep_words', 'table_name']
        df_concat = pd.concat([self.df_0_categ, self.df_1_categ]).loc[:, columns].reset_index(drop=True)
        df_concat.loc[:, 'product_name'] = df_concat.product_name.str.replace(' ', '')
        df_concat = df_concat[df_concat.product_name.str.len() >= 6].sort_values(['table_name', 'id']).reset_index(drop=True)

        df_concat.to_csv(self.tbl_deprepro, index=False)


class ThreadMapping(QtCore.QThread, QtCore.QObject):
    ''' Thread comparing product info '''
    
    def __init__(self, parent=None):
        super().__init__()
        self.power = False
        
        # file path
        self.tbl_deprepro = os.path.join(tbl_cache, 'tbl_deprepro.csv')
        self.mapping_table = os.path.join(tbl_cache, 'mapping_table.csv')
        
    progress = QtCore.pyqtSignal(object)
    def run(self):
        
        df_concat = pd.read_csv(self.tbl_deprepro)
        subset = ['brand_name', 'product_name', 'category']
        df_dup = df_concat[df_concat.duplicated(subset=subset, keep=False)].sort_values(by=subset).reset_index(drop=True)
        df_group = df_dup.groupby(subset)
        groups = df_group.groups.keys()
        mapping_list = []
        
        t = tqdm(groups)
        for group in t:
            self.progress.emit(t)
            if self.power:
                grp = df_group.get_group(group)
                
                if 'glowpick_product_info_final_version' in grp.table_name.tolist():
                    grp_0 = grp.loc[grp.table_name=='glowpick_product_info_final_version']
                    grp_1 = grp.loc[grp.table_name!='glowpick_product_info_final_version'].reset_index(drop=True)
                    item_key = grp_0.id.values[0]
                    keep_words_0 = grp_0.keep_words.values[0]
                    for i in range(len(grp_1)):
                        mapped_id = grp_1.loc[i, 'id']
                        keep_words_1 = grp_1.loc[i, 'keep_words']
                        table_name = grp_1.loc[i, 'table_name']
                        mapping_list.append([item_key, keep_words_0, mapped_id, keep_words_1, table_name])
            else:
                break
            
        self.progress.emit(t)
        self.poewr = False
            
        columns = ['item_key', 'item_keep_words', 'mapped_id', 'mapped_keep_words', 'source']
        mapping_table = pd.DataFrame(mapping_list, columns=columns).sort_values(['item_key', 'source'])
        mapping_table.to_csv(self.mapping_table, index=False)