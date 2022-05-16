import os
import re
import sys
import time
import pickle
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

# Exception Error Handling
import socket
import warnings
warnings.filterwarnings("ignore")

cur_dir = os.path.dirname(os.path.realpath(__file__))
root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
src = os.path.abspath(os.path.join(cur_dir, os.pardir))
sys.path.append(root)
sys.path.append(src)

from PyQt5 import QtCore
from PyQt5.QtWidgets import *

from hangle import _distance
from access_database import access_db
from scraping import scraper

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    conn_path = os.path.join(base_path, 'conn.txt')
    
else:
    base_path = os.path.dirname(os.path.realpath(__file__))
    tbl_cache = root + '/tbl_cache'
    conn_path = os.path.join(src, 'gui/conn.txt')
    
from scraping.scraper import ReviewScrapeNv
    
    
class CrawlingNvRev(QtCore.QThread, QtCore.QObject):
    ''' Thread scraping product info '''
    
    def __init__(self, parent=None):
        super().__init__()
        self.power = True
        self.path_crw = os.path.join(tbl_cache, 'df_for_rev_crw.csv')
        self.path_scrape_lst = os.path.join(tbl_cache, 'scrape_list.txt')
        self.path_scrape_df = os.path.join(tbl_cache, 'scrape_df.csv')
        self.path_status = os.path.join(tbl_cache, 'status_dict.txt')
        self.rs = ReviewScrapeNv()
        
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = access_db.AccessDataBase(conn[0], conn[1], conn[2])
        
    progress = QtCore.pyqtSignal(object)
    def run(self):
        ''' Run Thread '''
        
        # load data for crawling
        df_for_rev_crw = pd.read_csv(self.path_crw)
        if os.path.isfile(self.path_scrape_lst):
            with open(self.path_scrape_lst, 'rb') as f:
                df_list = pickle.load(f)
        else:
            df_list = []
            
        if os.path.isfile(self.path_status):
            with open(self.path_status, 'rb') as f:
                status_dict = pickle.load(f)
        else:
            status_dict = {}
            
        t = tqdm(range(len(df_for_rev_crw)))
        for idx in t:
            if self.power == True:
                # Run: 작업 수행
                self.progress.emit(t)
                
                # Naver price tab review crawling
                id_ = df_for_rev_crw.loc[idx, 'id']
                url = df_for_rev_crw.loc[idx, 'product_url']
                ratings, review_infos, review_texts, status = self.rs.review_crawler(url)
                _df = pd.DataFrame({"id" : id_, "rating": ratings, "review_infos": review_infos, "review_texts": review_texts, "status": status})
                # append DataFrame
                df_list.append(_df)
                # update status
                status_dict[id_] = status
                
            else:
                # Pause: 일시정지
                self.progress.emit(t)
                break
                
            # 이어서 작업 수행 하기 위해 캐시데이터 저장 
            # save df_for_rev_crw
            df_for_rev_crw.loc[idx:].reset_index(drop=True).to_csv(self.path_crw, index=False)
            
            # save scraping data
            with open(self.path_scrape_lst, 'wb') as f:
                pickle.dump(df_list, f)
            pd.concat(df_list).reset_index(drop=True).to_csv(self.path_scrape_df, index=False)
                
            # save status dict
            with open(self.path_status, 'wb') as f:
                pickle.dump(status_dict, f)
                
            # Crawl completed
            if idx == len(df_for_rev_crw) - 1:
                
                # assign DataFrame 
                ids = list(status_dict.keys())
                sts = list(status_dict.values())
                df_status = pd.DataFrame(columns=['id', 'status'])
                df_status.loc[:, 'id'] = ids
                df_status.loc[:, 'status'] = sts
            
                # table Update from db
                with open(tbl_cache + '/status_table_name.txt', 'wb') as f:
                    table_name = pickle.load(f)[0]
                pk = "id"
                self.db.table_update(table_name, pk, df_status)
                
    def stop(self):
        ''' Stop Thread '''
        
        self.power = False
        self.quit()
        self.wait(3000)