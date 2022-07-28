import os
import re
import sys
import time
import pickle
import pandas as pd
from tqdm.auto import tqdm
from datetime import datetime
from PyQt5 import QtCore

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

from access_database.access_db import AccessDataBase
from scraping.scraper import scraper_nv
from scraping.crawler_naver import ReviewScrapeNv
from mapping._preprocessing import TitlePreProcess
from scraping.crawler_naver import ProductStatusNv
from errors import Errors
    
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
        self.db = AccessDataBase(conn[0], conn[1], conn[2])
        
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
        
preprocessor = TitlePreProcess()
prd = ProductStatusNv()
class ThreadCrawlingNvStatus(QtCore.QThread, QtCore.QObject):
    def __init__(self):
        super().__init__()
        self.power = False
        self.check = 0
        
        # path
        self.path_input_df = os.path.join(tbl_cache, 'input_df.csv')
        self.path_store_list = os.path.join(tbl_cache, 'store_list.txt')
        self.path_status_list = os.path.join(tbl_cache, 'status_list.txt')
        self.path_scrape_df = os.path.join(tbl_cache, 'scrape_df.csv')
        self.category_list = os.path.join(tbl_cache, 'category_list.txt')
        
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = AccessDataBase(conn[0], conn[1], conn[2])
        
        # error class
        self.err = Errors()
        
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
        
        # category dict
        self.categ_dict = {
            '스킨케어': 'skin_care',
            '메이크업': 'makeup',
            '바디케어': 'body_care',
            '헤어케어': 'hair_care', 
            '선케어': 'sun_care', 
            '클렌징': 'cleansing',
            '맨즈케어': 'mens_care',
            '마스크/팩': 'mask_pack',
            '뷰티툴': 'beauty_tool',
            '프래그런스': 'fragrance',            
        }
    def _get_tbl(self):
        
        # naver
        table_list = self.db.get_tbl_name()
        reg = re.compile('naver_beauty_product_info_extended_v[0-9]+')
        tables = []
        for tbl in table_list:
            tbl_ = re.match(reg, tbl)
            if tbl_:
                tables.append(tbl_.group(0))
        tables = list(set(tables))
        columns = ['id', 'product_url', 'selection', 'division', 'groups']

        # check mapping products
        df = self.db.integ_tbl(tables, columns)
        mapping_table = self.db.get_tbl('beauty_kr_mapping_table', ['item_key', 'mapped_id', 'source']).rename(columns={'mapped_id': 'id', 'source': 'table_name'})
        df_mapped = df.merge(mapping_table, on=['id', 'table_name'], how='inner')
        df_mapped = preprocessor.categ_reclassifier(df_mapped)
        
        # check status
        status_df = self.db.get_tbl('naver_beauty_product_info_status')
        if status_df is None:
            pass
        else: 
            df_mapped = df_mapped.merge(status_df, on='product_url', how='left').fillna(-2)
            df_mapped = df_mapped.loc[df_mapped.status!=-1].reset_index(drop=True)
        
        return df_mapped

    def _get_category(self):
        # category 
        if os.path.isfile(self.category_list):
            with open(self.category_list, 'rb') as f:
                categs = pickle.load(f)
            self.categ = categs[0]
            self.categ_eng = self.categ_dict[self.categ]
        else:
            self.categ = '카테고리'
            self.categ_eng = 'category'
        
    def _upload_df(self, comp=False):
        ''' table upload into db '''
        
        # table name
        if comp:
            table_name = f'beauty_kr_{self.categ_eng}_info_all'
        else:    
            table_name = f'beauty_kr_{self.categ_eng}_info_all_temp'
        
        df = self.db.get_tbl(f'beauty_kr_{self.categ_eng}_info_all_temp')
        try:
            '''status table''' 
            status_df = pd.DataFrame(self.status_list, columns=['product_url', 'status'])
        
            # dup check
            _status_df = self.db.get_tbl('naver_beauty_product_info_status')
            if _status_df is None:
                status_df_dedup = status_df.copy()
            else:
                status_df_dedup = pd.concat([status_df, _status_df]).drop_duplicates('product_url', keep='first')
                
            # update table
            self.db.engine_upload(status_df_dedup, 'naver_beauty_product_info_status', 'replace')
            
            '''info table'''
            gl_info = self.db.get_tbl('glowpick_product_info_final_version', 'all').rename(columns={'id': 'item_key', 'product_url': 'product_url_glowpick'})
            columns = ['item_key', 'product_url', 'product_store', 'product_store_url', 'product_price', 'delivery_fee', 'naver_pay', 'product_status', 'page_status']
            nv_prd_status_update = pd.DataFrame(self.store_list, columns=columns)
            nv_prd_status_update.to_csv(self.path_scrape_df, index=False)
            
            _nv_prd_status_update = nv_prd_status_update[nv_prd_status_update.product_status==1]    # 판매 중 상품
            _nv_prd_status_update_price = _nv_prd_status_update.loc[_nv_prd_status_update.page_status==1]    # 네이버 뷰티윈도 가격비교 탭 상품
            _nv_prd_status_update_all = _nv_prd_status_update.loc[_nv_prd_status_update.page_status==2]    # 네이버 뷰티윈도 전체 탭 상품
            _nv_prd_status_update_dedup = pd.concat([_nv_prd_status_update_price, _nv_prd_status_update_all]).drop_duplicates('item_key', keep='first')
            
            df_mer = _nv_prd_status_update_dedup.merge(gl_info, on='item_key', how='left').sort_values(by='item_key').reset_index(drop=True)
            df_mer.loc[:, 'regist_date'] = pd.Timestamp(self.date)
            
            # category 
            df_mer.loc[:, 'category'] = self.categ
            
            # dedup
            if df is None:
                pass
            else:
                df_mer = pd.concat([df_mer, df]).drop_duplicates('item_key', keep='first').sort_values(by='item_key').reset_index(drop=True)
            
            # table upload
            if comp:
                self.db.create_table(df_mer, table_name)    
                os.remove('self.path_input_df')
                os.remove('self.path_status_list')
            else:                
                self.db.engine_upload(df_mer, table_name, 'replace')
            status = 1
        
        except Exception as e:
            # db 연결 끊김: 인터넷(와이파이) 재연결 필요
            print(e)
            if self.power:
                self.stop()
            self.check = 2
            status = -1
        
        return status, table_name
            
    progress = QtCore.pyqtSignal(object)
    def run(self):
        
        # input data (naver extended v[1:5] info)
        df = pd.read_csv(self.path_input_df)
        # store list
        if os.path.isfile(self.path_store_list):
            with open(self.path_store_list, 'rb') as f:
                self.store_list = pickle.load(f)
        else:
            self.store_list = []
        
        # status list
        if os.path.isfile(self.path_status_list):
            with open(self.path_status_list, 'rb') as f:
                self.status_list = pickle.load(f)
        else:
            self.status_list = []
        
        cnt = 0    
        t = tqdm(range(len(df)))
        for idx in t:
            if self.power:
                try:
                    self.check = 0
                    self.progress.emit(t)
                    
                    item_key = df.loc[idx, 'item_key']
                    url = df.loc[idx, 'product_url']
                    
                    st = time.time()
                    
                    # errors log
                    try:
                        product_status, store_info = prd.scraping_product_stores(item_key, url, None, None)
                        self.status_list.append([url, product_status])
                    except:
                        store_info = None
                        self.err.errors_log(url)
                        
                    ed = time.time()
                    if ed - st > 100:
                        # 네이버 VPM ip 차단
                        self.check = 1
                        break
                    
                    if store_info == None:
                        pass
                    else:
                        self.store_list.append(store_info)
                    cnt += 1  
                except:
                    break
            else:
                break
            
        # save data into cache dir
        df.loc[cnt:].to_csv(self.path_input_df)
        with open(self.path_store_list, 'wb') as f:
            pickle.dump(self.store_list, f)
        with open(self.path_status_list, 'wb') as f:
            pickle.dump(self.status_list, f)
        
        # upload table into db
        if cnt == len(df):
            self._upload_df(comp=True)
        else:
            self._upload_df()
        
        self.progress.emit(t)
        self.power = False
        
    def stop(self):
        ''' Stop Thread '''
        
        self.power = False
        self.quit()
        self.wait(3000)
        
class ThreadCrawlingNvInfo(QtCore.QThread, QtCore.QObject):
    ''' Thread scraping naver product info '''
    
    def __init__(self, parent=None):
        super().__init__()
        self.power = True
        
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = AccessDataBase(conn[0], conn[1], conn[2])
        
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

                outputs = scraper_nv(id_, search_words)
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
                
                self.progress.emit(t)
                break
            
        if idx == len(prds) - 1:
            # 스크레이핑 데이터 할당 데이터프레임
            columns = ['id','input_words','product_name','product_url','price','category','product_description','registered_date','product_reviews_count','product_rating','product_store','similarity']
            df = pd.DataFrame(scrap_list, columns=columns)
            df.to_csv(tbl_cache + '/df_info_scrap.csv', index=False)
        
        self.progress.emit(t)
        self.power = False
    
    def stop(self):
        ''' Stop Thread '''
        
        self.power = False
        self.quit()
        self.wait(3000)