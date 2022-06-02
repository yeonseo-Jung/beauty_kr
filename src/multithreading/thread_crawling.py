import os
import re
import sys
import time
import pickle
import numpy as np
import pandas as pd
from tqdm.auto import tqdm
from datetime import datetime

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

from access_database import access_db
from scraping.scraper import get_url
from scraping.scraper import ReviewScrapeNv
from scraping.scraper import CrawlInfoRevGl
from mapping._preprocessing import TitlePreProcess
from scraping.crawler_naver import ProductStatusNv

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    conn_path = os.path.join(base_path, 'conn.txt')
    
else:
    base_path = os.path.dirname(os.path.realpath(__file__))
    tbl_cache = root + '/tbl_cache'
    conn_path = os.path.join(src, 'gui/conn.txt')
    

    
class ThreadCrawlingNvRev(QtCore.QThread, QtCore.QObject):
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
                
                # Pause: 일시정지    
                self.progress.emit(t)
                break
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
        
        
crw = CrawlInfoRevGl()
class ThreadCrawlingGl(QtCore.QThread, QtCore.QObject):
    ''' Thread Crawling glowpick products '''
    
    def __init__(self, parent=None):
        super().__init__()
        self.power = False
        self.check = 0
        self.file_path = os.path.join(tbl_cache, 'product_codes.txt')
        self.path_scrape_df = os.path.join(tbl_cache, 'gl_info.csv')
        self.path_scrape_df_rev = os.path.join(tbl_cache, 'gl_info_rev.csv')
        
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = access_db.AccessDataBase(conn[0], conn[1], conn[2])
        
        # today (regist date)
        today = datetime.today()
        year = str(today.year)
        month = str(today.month)
        day = str(today.day)
        if len(month) == 1:
            month = "0" + month
        if len(day) == 1:
            day = "0" + day
        self.date = year + "-" + month + "-" + day
        date = year[2:4] + month + day
        # table name
        self.table_name_info = f"glowpick_product_info_update_{date}"
        self.table_name_rev = f"glowpick_product_info_update_review_{date}"
        self.table_name_status = f"glowpick_product_info_update_status_{date}"
        
    def _get_tbl(self):
        
        tables = ['glowpick_product_info_final_version']
        columns = ['id', 'product_code', 'selection']

        df = self.db.integ_tbl(tables, columns)
        mapping_table = self.db.get_tbl('beauty_kr_mapping_table', ['item_key'])
        item_keys = mapping_table.item_key.unique().tolist()
        df_mapped = df.loc[df.id.isin(item_keys)].reset_index(drop=True)
        
        return df_mapped
    
    def _upload_df(self):
        ''' Upload Table to Database '''
        
        if (len(self.scrape_infos) != 0) & (len(self.scrape_reviews) != 0):
            # info table
            columns = ['product_code', 'product_name', 'brand_code', 'brand_name', 'product_url',
                        'selection', 'division', 'groups', 
                        'descriptions', 'product_keywords', 'color_type', 'volume', 'image_source', 
                        'ingredients_all_kor', 'ingredients_all_eng', 'ingredients_all_desc',
                        'ranks', 'product_awards', 'product_awards_sector', 'product_awards_rank',
                        'price', 'product_stores']
            df_info = pd.DataFrame(self.scrape_infos, columns=columns)
            df_info.loc[:, 'regist_date'] = self.date
            df_info.to_csv(self.path_scrape_df, index=False)
            
            # reivew table
            columns = ['product_code', 'user_id', 'product_rating', 'review_date', 'product_review']
            df_rev = pd.DataFrame(self.scrape_reviews, columns=columns)
            df_rev.to_csv(self.path_scrape_df_rev, index=False)
            
            # status table
            df_status = pd.DataFrame(self.status_list, columns=['product_code', 'status'])
            
            # Upload Database
            try:
                self.db.engine_upload(df_info, self.table_name_info, 'append')
                self.db.engine_upload(df_rev, self.table_name_rev, 'append')
                self.db.engine_upload(df_status, self.table_name_status, 'append')
            except:
                # db 연결 끊김: 인터넷(와이파이) 재연결 필요
                # self.stop()
                self.check = 1
        
    progress = QtCore.pyqtSignal(object)
    def run(self):
        ''' Run Thread '''
        
        review_check = 1
        with open(self.file_path, 'rb') as f:
            product_codes = pickle.load(f)
        self.scrape_infos, self.scrape_reviews, self.status_list = [], [], []
        idx = 0
        t = tqdm(product_codes)
        for code in t:
            if self.power:
                self.progress.emit(t)
            
                driver, status = crw.get_webdriver_gl(code)
                if status == -1:
                    # 글로우픽 VPN ip 차단: VPN 재연결 필요
                    self.stop()
                    self.check = 2
                    
                elif status == 1:
                    scrape, status, driver = crw.scrape_gl_info(code, driver, review_check)
                    
                    if status == 1:
                        self.scrape_infos.append(scrape)
                        if review_check == 1:
                            reviews, rev_status = crw.crawling_review(code, driver)
                            if rev_status == 1:
                                self.scrape_reviews += reviews
                            
                self.status_list.append([code, status])
                
                if len(self.scrape_infos) % 250 == 0:
                    self._upload_df()
                    
            else:
                break
            idx += 1
            
        with open(self.file_path, 'wb') as f:
            pickle.dump(product_codes[idx:], f)
        self._upload_df()
        self.progress.emit(t)
        self.power = False
                
    def stop(self):
        ''' Stop Thread '''
        
        self.power = False
        self.quit()
        self.wait(3000)
        
prd = ProductStatusNv()
class ThreadCrawlingProductCode(QtCore.QThread, QtCore.QObject):
    ''' Thread Crawling glowpick products '''
    
    def __init__(self, parent=None):
        super().__init__()
        self.power = False
        self.check = 0
        self.file_path = os.path.join(tbl_cache, 'product_codes.txt')
        self.selections = os.path.join(tbl_cache, 'selections.txt')
        self.divisions = os.path.join(tbl_cache, 'divisions.txt')
        self.selection_idx = os.path.join(tbl_cache, 'selection_idx.txt')
        self.division_idx = os.path.join(tbl_cache, 'division_idx.txt')
        
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = access_db.AccessDataBase(conn[0], conn[1], conn[2])
        
    def find_category_index(self):
        ''' Crawling & Save category index dictionary '''
        
        if os.path.isfile(self.selection_idx):
            with open(self.selection_idx, 'rb') as f:
                selelction_idx = pickle.load(f)
            with open(self.division_idx, 'rb') as f:
                division_idx = pickle.load(f)
        else:        
            selelction_idx = crw.find_selection_new()
            division_idx = crw.find_division_rank()
            with open(self.selection_idx, 'wb') as f:
                pickle.dump(selelction_idx, f)
            with open(self.division_idx, 'wb') as f:
                pickle.dump(division_idx, f)
            
    progress = QtCore.pyqtSignal(object)
    def run(self):
        ''' Run Thread '''
                
        # Category index
        with open(self.selection_idx, 'rb') as f:
            selelction_idx = pickle.load(f)
        with open(self.division_idx, 'rb') as f:
            division_idx = pickle.load(f)
        
        # Categories to crawl
        with open(self.selections, 'rb') as f:
            selections = pickle.load(f)
        with open(self.divisions, 'rb') as f:
            divisions = pickle.load(f)
        
        sel_idx, div_idx = [], []
        for sel in selections:
            if sel in selelction_idx.keys():
                sel_idx.append(selelction_idx[sel])
        for div in divisions:
            if div in division_idx.keys():
                div_idx.append(division_idx[div])
        
        urls = []
        t = tqdm(range(len(sel_idx) + len(div_idx)))
        for i in t:
            if self.power:
                self.progress.emit(t)
            
                if i < len(sel_idx):
                    # Scraping rank products
                    idx = sel_idx[i]
                    url = f"https://www.glowpick.com/categories/{idx}?tab=ranking"    # glowpick ranking products page 
                    wd = get_url(url)
                    urls += crw.scraping_prds_rank(wd)
                    
                else:
                    # Scraping rank products
                    # Scraping new products
                    i -= len(sel_idx)
                    idx = div_idx[i]
                    url = f"https://www.glowpick.com/products/brand-new?cate1Id={idx}"    # glowpick new products page 
                    wd = get_url(url)
                    urls += crw.scraping_prds_new(wd)
            else:
                break
                
        # url -> product_code
        product_codes = []
        for url in urls:
            product_code = url.replace('https://www.glowpick.com/products/', '')
            product_codes.append(product_code)
        with open(self.file_path, 'wb') as f:
            pickle.dump(product_codes, f)
        self.power = False
        self.progress.emit(t)
                
    def stop(self):
        ''' Stop Thread '''
        
        self.power = False
        self.quit()
        self.wait(3000)
        
preprocessor = TitlePreProcess()
class ThreadCrawlingNvStatus(QtCore.QThread, QtCore.QObject):
    def __init__(self):
        super().__init__()
        self.power = False
        self.check = 0
        
        # path
        self.path_input_df = os.path.join(tbl_cache, 'input_df.csv')
        self.store_list = os.path.join(tbl_cache, 'store_list.txt')
        self.status_list = os.path.join(tbl_cache, 'status_list.txt')
        self.path_scrape_df = os.path.join(tbl_cache, 'scrape_df.csv')
        
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = access_db.AccessDataBase(conn[0], conn[1], conn[2])   
        
        # today (regist date)
        today = datetime.today()
        year = str(today.year)
        month = str(today.month)
        day = str(today.day)
        if len(month) == 1:
            month = "0" + month
        if len(day) == 1:
            day = "0" + day
        self.date = year + "-" + month + "-" + day
        date = year[2:4] + month + day
        # uploaded table name
        self.table_name = f'beauty_kr_product_info_{date}'
    
    def _get_tbl(self):
        
        tables = ['naver_beauty_product_info_extended_v1', 'naver_beauty_product_info_extended_v2', 'naver_beauty_product_info_extended_v3', 'naver_beauty_product_info_extended_v4', 'naver_beauty_product_info_extended_v5']
        columns = ['id', 'product_url', 'selection', 'division', 'groups']

        df = self.db.integ_tbl(tables, columns)
        mapping_table = self.db.get_tbl('beauty_kr_mapping_table', ['item_key', 'mapped_id', 'source']).rename(columns={'mapped_id': 'id', 'source': 'table_name'})
        df_mapped = df.merge(mapping_table, on=['id', 'table_name'], how='inner')
        df_mapped = preprocessor.categ_reclassifier(df_mapped, source=1)
        
        return df_mapped
    
    def _upload_df(self):
        ''' table upload into db '''
        gl_info = self.db.get_tbl('glowpick_product_info_final_version', 'all').rename(columns={'id': 'item_key'})
        columns = ['item_key', 'product_store', 'product_stote_url', 'price', 'delivery_fee', 'naver_pay', 'product_status', 'page_status']
        nv_prd_status_update = pd.DataFrame(self.store_list, columns=columns).drop_duplicates(subset=['item_key', 'product_store', 'price', 'delivery_fee'], keep='first')
        nv_prd_status_update.to_csv(self.path_scrape_df)
        _nv_prd_status_update = nv_prd_status_update.loc[nv_prd_status_update.page_status==1]
        df_mer = _nv_prd_status_update.merge(gl_info, on='item_key', how='left').sort_values(by='item_key').reset_index(drop=True)
        
        # table upload
        try:
            self.db.engine_upload(df_mer, self.table_name, 'replace')
        except:
            # db 연결 끊김
            self.check = 1
            
    progress = QtCore.pyqtSignal(object)
    def run(self):
        
        # input data (naver extended v[1:5] info)
        df = pd.read_csv(self.path_input_df)
        # store list
        if os.path.isfile(self.store_list):
            with open(self.store_list, 'rb') as f:
                self.store_list = pickle.load(f)
        else:
            self.store_list = []
        # status list
        if os.path.isfile(self.status_list):
            with open(self.status_list, 'rb') as f:
                status_list = pickle.load(f)
        else:
            status_list = []
        
        if os.path.isfile(tbl_cache + '/prg_dict.txt'):
            os.remove(tbl_cache + '/prg_dict.txt')
            
        t = tqdm(range(len(df)))
        for idx in t:
            if self.power:
                self.progress.emit(t)
                
                item_key = df.loc[idx, 'item_key']
                url = df.loc[idx, 'product_url']
                product_status, store_info = prd.scraping_product_stores(item_key, url, None, None)
                if store_info == None:
                    pass
                else:
                    self.store_list.append(store_info)      
            else:
                break
            idx += 1
            
        # save ipunt data into cache dir
        df.loc[idx:].to_csv(self.path_input_df)
        
        # upload table into db 
        self._upload_df()
        
        self.power = False
        self.progress.emit(t)
                
    def stop(self):
        ''' Stop Thread '''
        
        self.power = False
        self.quit()
        self.wait(3000)