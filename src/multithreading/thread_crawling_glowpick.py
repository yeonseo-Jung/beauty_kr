import os
import sys
import pickle
import numpy as np
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
from scraping.scraper import get_url
from scraping.crawler_glowpick import CrawlInfoRevGl
from mapping._preprocessing import grouping
from errors import Errors
          
crw = CrawlInfoRevGl()
class ThreadCrawlingGl(QtCore.QThread, QtCore.QObject):
    ''' Thread Crawling glowpick products '''
    
    def __init__(self, parent=None):
        super().__init__()
        self.power = False
        self.check = 0
        
        # init db
        self.init_db()
        
        # scrape list
        self.scrape_infos, self.scrape_reviews, self.status_list = [], [], []
        
        # file path
        self.file_path = os.path.join(tbl_cache, 'product_codes.txt')
        self.path_scrape_df = os.path.join(tbl_cache, 'gl_info.csv')
        self.path_scrape_df_rev = os.path.join(tbl_cache, 'gl_info_rev.csv')
        self.scrape_infos_path = os.path.join(tbl_cache, 'scrape_infos.txt')
        self.scrape_reviews_path = os.path.join(tbl_cache, 'scrape_reviews.txt')
        
        
        # error class
        self.err = Errors()
    
    def init_db(self):
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = AccessDataBase(conn[0], conn[1], conn[2])
        
    def _get_tbl(self):
        
        tables = ['glowpick_product_info_final_version']
        columns = ['id', 'product_code', 'selection']

        try:
            df = self.db.integ_tbl(tables, columns)
            mapping_table = self.db.get_tbl('beauty_kr_mapping_table', ['item_key'])
            item_keys = mapping_table.item_key.unique().tolist()
            df_mapped = df.loc[df.id.isin(item_keys)].reset_index(drop=True)
        except:
            # db 연결 끊김: VPN 연결 해제 및 와이파이 재연결 필요
            df_mapped = pd.DataFrame()
            self.err.errors_log()
        
        return df_mapped
    
    def _preprocess(self):
        df_info, df_rev_dedup = None, None
        if (len(self.scrape_infos) != 0) & (len(self.scrape_reviews) != 0):
            # info table
            columns = ['product_code', 'product_url', 'product_name', 'brand_name', 'brand_code', 'groups', 'product_awards', 'ingredients_all_kor', 'rating', 'review_counts', 'offers']
            info_df = pd.DataFrame(self.scrape_infos, columns=columns)
            info_df.loc[info_df.product_awards=="[]", 'product_awards'] = None
            info_df.loc[info_df.ingredients_all_kor=="[]", 'ingredients_all_kor'] = None
            price_size_df = pd.DataFrame(info_df.offers.tolist())
            concat_df = pd.concat([info_df, price_size_df], axis=1)
            
            # insert category (selection, division, groups)
            query = "SELECT DISTINCT selection, division, groups FROM glowpick_product_info_final_version"
            data = self.db._execute(query)
            categ_df = pd.DataFrame(data)
            concat_df_notnull = concat_df[concat_df.groups.notnull()].reset_index(drop=True)
            
            # merge (insert data)
            mer_df = concat_df_notnull.merge(categ_df, on="groups", how="left")
            mer_df_notnull = mer_df[mer_df.selection.notnull()]
            mer_df_null = mer_df[mer_df.selection.isnull()].drop(columns="selection")
            mer_df_null.loc[:, "division"] = mer_df_null.loc[:, "groups"]
            mer_df_null.loc[:, "groups"] = None
            mer_df_null = mer_df_null.merge(categ_df.loc[:, ["selection", "division"]], on="division",  how="inner")
            concat_df = pd.concat([mer_df_notnull, mer_df_null], ignore_index=True)
            
            # dedup & rename & save
            columns = ['product_code', 'product_name', 'product_url', 'brand_name', 'brand_code', 'selection', 'division', 'groups', 'product_awards', 'ingredients_all_kor', 'price', 'size']
            df_info = concat_df.loc[:, columns].rename(columns={"size": "volume"})
            df_info = df_info.drop_duplicates('product_code', keep='first', ignore_index=True)
            df_info.loc[:, 'regist_date'] = pd.Timestamp(datetime.today().strftime("%Y-%m-%d"))
            df_info.to_csv(self.path_scrape_df, index=False)
            
            # reivew table
            columns = ['product_code', 'user_id', 'product_rating', 'review_date', 'product_review']
            df_rev = pd.DataFrame(self.scrape_reviews, columns=columns)
            df_rev = df_rev.loc[df_rev.product_code.isin(df_info.product_code.unique())]
            df_rev.loc[df_rev.product_review==''] = None
            df_rev = df_rev.loc[df_rev.product_review.notnull()]
            df_rev_dedup = df_rev.drop_duplicates(subset=columns, keep='first', ignore_index=True)
            df_rev_dedup.loc[:, 'regist_date'] = pd.Timestamp(datetime.today().strftime("%Y-%m-%d"))
            df_rev_dedup.to_csv(self.path_scrape_df_rev, index=False)
        else:
            print("\n\n** 수집된 데이터가 없습니다.\nds > beauty_kr > `error_log`를 확인해주세요. **\n\n")
            
        return df_info, df_rev_dedup
            
    def _upload_df(self, comp=False):
        ''' Upload Table to Database '''
        
        df_info, df_rev = self._preprocess()
        if df_info is not None and df_rev is not None:
            try:
                # re-connect db
                self.init_db()
                if comp:
                    ''' Table Update (append) '''
                    os.remove(self.file_path)
                    
                    # glowpick_product_info_final_version
                    gl_info_final_v = self.db.get_tbl('glowpick_product_info_final_version', 'all')
                    
                    # 기존 상품 id 부여
                    df_mer = gl_info_final_v.loc[:, ['id', 'product_code']].merge(df_info, on='product_code', how='inner')
                    
                    # 기존상품 추출
                    df_dedup = pd.concat([gl_info_final_v, df_mer]).drop_duplicates('id', keep='first').sort_values('id')
                    
                    # 신규상품 추출
                    gl_info_new_v = pd.concat([df_mer, df_info]).drop_duplicates('product_code', keep=False, ignore_index=True)
                    
                    # 신규 상품 id 부여
                    gl_info_new_v.loc[:, 'id'] = range(len(df_dedup), len(df_dedup) + len(gl_info_new_v))
                    _gl_info_final_v = pd.concat([df_dedup, gl_info_new_v], ignore_index=True)
                    gl_dup_ck = grouping(_gl_info_final_v.loc[:, ['id', 'product_name', 'product_code', 'brand_code']])    # dup check 
                    if 'status' in _gl_info_final_v.columns:
                        _gl_info_final_v = _gl_info_final_v.drop(columns=['status', 'dup_check', 'dup_id'])
                    _gl_info_final_v_dedup = _gl_info_final_v.merge(gl_dup_ck, on='id', how='inner')
                    
                    # glowpick_product_info_final_version_review
                    gl_rev_final_v = self.db.get_tbl('glowpick_product_info_final_version_review')
                    df_mer_rev = _gl_info_final_v_dedup.loc[:, ['id', 'product_code']].merge(df_rev, on='product_code', how='inner')
                    subset = ['product_code', 'user_id', 'product_rating', 'review_date', 'product_review']
                    df_dedup_rev = pd.concat([df_mer_rev, gl_rev_final_v]).drop_duplicates(subset=subset, keep='first').sort_values(by='id', ignore_index=True)
                    df_dedup_rev = df_dedup_rev.loc[df_dedup_rev.product_review.notnull()].reset_index(drop=True)
                    
                    try:
                        # upload table into db
                        if gl_info_new_v.empty:
                            pass
                        else:
                            gl_info_new_v.loc[:, 'crawling_status'] = 0
                            self.db.create_table(gl_info_new_v, 'glowpick_product_info_update_new', append=True)
                        self.db.create_table(_gl_info_final_v_dedup, 'glowpick_product_info_final_version')
                        self.db.create_table(df_dedup_rev, 'glowpick_product_info_final_version_review')
                        
                        # init cache file
                        self.scrape_infos, self.scrape_reviews, self.status_list = [], [], []
                        
                        return 1
                    except:
                        # db 연결 끊김: VPN 연결 해제 및 와이파이 재연결 필요
                        self.err.errors_log()
                        self.init_db()
                        self.check = 2
                        return -1
                else:
                    # 수집 도중 일시정지 되었습니다.
                    # 데이터 임시저장 및 백업 진행됩니다.
                    with open(self.scrape_infos_path, 'wb') as f:
                        pickle.dump(self.scrape_infos, f)
                        
                    with open(self.scrape_reviews_path, 'wb') as f:
                        pickle.dump(self.scrape_reviews, f)
                    try:
                        self.db.engine_upload(df_info, "glowpick_product_info_final_version_temp", if_exists_option="append")
                        self.db.engine_upload(df_info, "glowpick_product_info_final_version_review_temp", if_exists_option="append")
                    except:
                        # db 연결 끊김: VPN 연결 해제 및 와이파이 재연결 필요
                        self.err.errors_log()
                        self.init_db()
                        self.check = 2
                        return -1
                    return 2
            except:
                # db 연결 끊김: VPN 연결 해제 및 와이파이 재연결 필요
                self.err.errors_log()
                self.init_db()
                self.check = 2
                if self.power:
                    self.stop()
                return -2
        else:
            return 0
                
    progress = QtCore.pyqtSignal(object)
    def run(self):
        ''' Run Thread '''
        
        review_check = 1 # 리뷰 수집 여부
        
        # 일시정지 후 이어서 수집할 때
        with open(self.file_path, 'rb') as f:
            product_codes = pickle.load(f)
        
        if os.path.exists(self.scrape_infos_path):
            with open(self.scrape_infos_path, 'rb') as f:
                self.scrape_infos = pickle.load(f)
        
        if os.path.exists(self.scrape_reviews_path):
            with open(self.scrape_reviews_path, 'rb') as f:
                self.scrape_reviews = pickle.load(f)
            
        idx = 0
        t = tqdm(product_codes)
        for code in t:
            if self.power:
                self.check = 0
                self.progress.emit(t)
            
                driver, status = crw.get_webdriver_gl(code)
                if status == -1:
                    # 글로우픽 VPN ip 차단: VPN 재연결 필요
                    self.check = 1
                    break                    
                
                elif status == 1:
                    try:
                        scrape, status, driver = crw.scrape_gl_info(code, driver)
                        
                        if status == 1:
                            self.scrape_infos.append(scrape)
                            if review_check == 1:
                                reviews, rev_status = crw.crawling_review(code, driver)
                                if rev_status == 1:
                                    self.scrape_reviews += reviews
                            else:
                                driver.quit()
                        else:
                            driver.quit()
                    except:
                        url = f'https://www.glowpick.com/products/{code}'
                        self.err.errors_log(url)
                        status = -1
                            
                self.status_list.append([code, status])
                idx += 1
            else:
                break
        
        # save ipunt data into cache dir
        with open(self.file_path, 'wb') as f:
            pickle.dump(product_codes[idx:], f)
        
        if idx == len(product_codes):
            # Thread completion
            self._upload_df(comp=True)
        else:
            # upload table into db 
            self._upload_df()
        
        self.progress.emit(t)
        self.power = False
                
    def stop(self):
        ''' Stop Thread '''
        
        self.power = False
        self.quit()
        self.wait(3000)
        
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
        self.db = AccessDataBase(conn[0], conn[1], conn[2])
        
        # error log
        self.err = Errors()
        
    def find_category_index(self):
        ''' Crawling & Save category index dictionary '''
        
        ck = 0
        if os.path.isfile(self.selection_idx):
            with open(self.selection_idx, 'rb') as f:
                selelction_idx = pickle.load(f)
            with open(self.division_idx, 'rb') as f:
                division_idx = pickle.load(f)
            if len(selelction_idx) == 0:
                pass
            else:
                ck = 1
                
        if ck == 0:
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
            
                try:
                    if i < len(sel_idx):
                        # Scraping rank products
                        idx = sel_idx[i]
                        url = f"https://www.glowpick.com/categories/{idx}?tab=ranking"    # glowpick ranking products page 
                        wd = get_url(url)
                        urls += crw.scraping_prds_rank(wd)
                        
                    else:
                        # Scraping new products
                        i -= len(sel_idx)
                        idx = div_idx[i]
                        url = f"https://www.glowpick.com/products/brand-new?cate1Id={idx}"    # glowpick new products page 
                        wd = get_url(url)
                        urls += crw.scraping_prds_new(wd)
                except:
                    self.err.errors_log(url)        
            else:
                break
                
        # url -> product_code
        # dedup
        urls = list(set(urls))    
        product_codes = []
        for url in urls:
            product_code = url.replace('https://www.glowpick.com/products/', '')
            product_codes.append(product_code)
        with open(self.file_path, 'wb') as f:
            pickle.dump(product_codes, f)
        
        self.progress.emit(t)
        self.power = False
                
    def stop(self):
        ''' Stop Thread '''
        
        self.power = False
        self.quit()
        self.wait(3000)