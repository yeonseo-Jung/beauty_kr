import os
import re
import sys
import time
import pickle
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

# Scrapping
from bs4 import BeautifulSoup
from selenium import webdriver
from fake_useragent import UserAgent
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


# Exception Error Handling
import socket
import warnings
warnings.filterwarnings("ignore")
from selenium.common.exceptions import WebDriverException, TimeoutException
from requests.exceptions import SSLError, ConnectionError, Timeout, ReadTimeout, RequestException
from fake_useragent.errors import FakeUserAgentError

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


class ThreadScraping(QtCore.QThread, QtCore.QObject):
    ''' Thread scraping product info '''
    
    def __init__(self, parent=None):
        super().__init__()
        self.power = True
        
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = access_db.AccessDataBase(conn[0], conn[1], conn[2])
        
    progress = QtCore.pyqtSignal(object)
    def run(self):
        ''' 브랜드명 + 상품명으로 상품정보 스크래핑해서 데이터프레임에 할당 '''
        
        if os.path.isfile(tbl_cache + '/prds_scrap_.csv'):
            prds = pd.read_csv(tbl_cache + '/prds_scrap_.csv')    
        else:
            prds = pd.read_csv(tbl_cache + '/prds_scrap.csv')
            
        if os.path.isfile(tbl_cache + '/scrap_list.txt'):
            with open(tbl_cache + '/scrap_list.txt', 'rb') as f:
                scrap_list = pickle.load(f)
        else:
            scrap_list = []  
            
        if os.path.isfile(tbl_cache + '/status_dict.txt'):
            with open(tbl_cache + '/status_dict.txt', 'rb') as f:
                status_dict = pickle.load(f)
        else:
            status_dict = {}
        
        t = tqdm(range(len(prds)))
        for idx in t:
            if self.power == True:
                # Run: 작업 수행
                self.progress.emit(t)
                
                search_words = prds.loc[idx, 'brand_name'] + ' ' + prds.loc[idx, 'product_name']
                id_ = prds.loc[idx, 'id']

                outputs = scraper.scraper_nv(id_, search_words)
                scrap_list += outputs[0]
                status = outputs[1]
                
                status_dict[id_] = status

                # length 100개 마다 캐시에 저장 
                if len(scrap_list) % 100 == 0:
                    with open(tbl_cache + '/scrap_list.txt', 'wb') as f:
                        pickle.dump(scrap_list ,f)
                        
                    with open(tbl_cache + '/status_dict.txt', 'wb') as f:
                        pickle.dump(status_dict ,f)
                    
            else:
                # Pause: 이어서 작업 수행 하기 위해 캐시데이터 저장 
                
                prds_ = prds.loc[idx:].reset_index(drop=True)
                prds_.to_csv(tbl_cache + '/prds_scrap_.csv', index=False)
                
                with open(tbl_cache + '/scrap_list.txt', 'wb') as f:
                    pickle.dump(scrap_list ,f)
                    
                columns = ['id','input_words','product_name','product_url','price','category','product_description','registered_date','product_reviews_count','product_rating','product_store','similarity']
                df = pd.DataFrame(scrap_list, columns=columns)
                df.to_csv(tbl_cache + '/df_info_scrap.csv', index=False)
                
                with open(tbl_cache + '/status_dict.txt', 'wb') as f:
                    pickle.dump(status_dict, f)
                    
                # status 데이터 할당 데이터프레임
                ids = list(status_dict.keys())
                sts = list(status_dict.values())
                df_ = pd.DataFrame(columns=['id', 'status'])
                df_.loc[:, 'id'] = ids
                df_.loc[:, 'status'] = sts
            
                # table Update from db: glowpick_product_scrap_status 
                table_name = "glowpick_product_scrap_status"
                pk = "id"
                self.db.table_update(table_name, pk, df_)
                
                self.progress.emit(t)
                
                break
            
        if idx == len(prds) - 1:
            # 스크레이핑 데이터 할당 데이터프레임
            columns = ['id','input_words','product_name','product_url','price','category','product_description','registered_date','product_reviews_count','product_rating','product_store','similarity']
            df = pd.DataFrame(scrap_list, columns=columns)
            df.to_csv(tbl_cache + '/df_info_scrap.csv', index=False)
            
            # status 데이터 할당 데이터프레임
            ids = list(status_dict.keys())
            sts = list(status_dict.values())
            df_ = pd.DataFrame(columns=['id', 'status'])
            df_.loc[:, 'id'] = ids
            df_.loc[:, 'status'] = sts
        
            # table Update from db: glowpick_product_scrap_status 
            table_name = "glowpick_product_scrap_status"
            pk = "id"
            self.db.table_update(table_name, pk, df_)

    def stop(self):
        ''' Stop Thread '''
        
        self.power = False
        self.quit()
        self.wait(3000)