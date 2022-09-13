import os
import re
import sys
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

from PyQt5 import QtCore
from mapping._preprocessing import TitlePreProcess

preprocessor = TitlePreProcess()
class ThreadTitlePreprocess(QtCore.QThread, QtCore.QObject):
    
    ''' Thread preprocessing product name '''
    
    def __init__(self, parent=None):
        super().__init__()
        self.power = False
        
        # file path
        # self.tbl_0 = os.path.join(tbl_cache, 'tbl_0.csv')
        # self.tbl_1 = os.path.join(tbl_cache, 'tbl_1.csv')
        self.tbl = os.path.join(tbl_cache, 'tbl.csv')
        self.tbl_deprepro = os.path.join(tbl_cache, 'tbl_deprepro.csv')
        
    def _categ_reclaasify(self):
        '''reclassifying category'''        
        df = pd.read_csv(self.tbl)
        df_categs = preprocessor.categ_reclassifier(df)
        return df_categs
       
    progress = QtCore.pyqtSignal(object) 
    def run(self):
        
        df_categs = self._categ_reclaasify()
        t = tqdm(range(len(df_categs)))
        for idx in t:
            if self.power:
                self.progress.emit(t)
                
                title = df_categs.loc[idx, 'product_name']
                brand = df_categs.loc[idx, 'brand_name']
                title_, keep_wd_dict = preprocessor.title_preprocessor(title, brand)
                df_categs.loc[idx, 'product_name'] = str(title_)
                if len(keep_wd_dict) == 0:
                    df_categs.loc[idx, 'keep_words'] = np.nan
                else:
                    df_categs.loc[idx, 'keep_words'] = str(keep_wd_dict)        
            else:
                break
            
        self.progress.emit(t)
        self.power = False
        
        columns = ['id', 'brand_name', 'product_name', 'category', 'keep_words', 'table_name']
        df_categs = df_categs.loc[:, columns]
        df_categs.loc[:, 'product_name'] = df_categs.product_name.str.replace(' ', '')
        df_categs = df_categs[df_categs.product_name.str.len() >= 6].sort_values(['table_name', 'id']).reset_index(drop=True)
        df_categs.to_csv(self.tbl_deprepro, index=False)

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