import os
import re
import sys
import ast
import time
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
from scraping.crawler_naver import ReviewScrapeNv
from mapping._preprocessing import TitlePreProcess
from scraping.crawler_naver import ProductStatusNv, crawler_nv
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
        tables.append('naver_beauty_product_info_final_version')
        columns = ['id', 'product_url', 'selection', 'division', 'groups']
        df = self.db.integ_tbl(tables, columns) 

        # check mapping products
        mapping_table = self.db.get_tbl('beauty_kr_mapping_table', ['item_key', 'mapped_id', 'source']).rename(columns={'mapped_id': 'id', 'source': 'table_name'})
        df_mapped = df.merge(mapping_table, on=['id', 'table_name'], how='inner')
        df_mapped = preprocessor.categ_reclassifier(df_mapped)
        
        # check status
        status_df = self.db.get_tbl('naver_beauty_product_info_status')
        if status_df is None:
            pass
        else:
            # status -2(모름), status 0(일시품절), 1(판매중)만 추출
            df_mapped = df_mapped.merge(status_df, on='product_url', how='left')
            df_mapped = df_mapped.loc[(df_mapped.status==-2) | (df_mapped.status==1) | (df_mapped.status==0) | (df_mapped.status.isnull())].reset_index(drop=True)
        
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
            
    def _preprocess(self):
        
        '''status table''' 
        status_df = pd.DataFrame(self.status_list, columns=['product_url', 'status'])
    
        # dup check
        _status_df = self.db.get_tbl('naver_beauty_product_info_status')
        if _status_df is None:
            status_df_dedup = status_df.copy()
        else:
            status_df_dedup = pd.concat([status_df, _status_df]).drop_duplicates('product_url', keep='first')
        
        '''info table'''
        gl_info = self.db.get_tbl('glowpick_product_info_final_version', 'all').rename(columns={'id': 'item_key', 'product_url': 'product_url_glowpick'})
        columns = ['item_key', 'product_url', 'product_store', 'product_store_url', 'product_price', 'delivery_fee', 'naver_pay', 'product_status', 'page_status']
        nv_prd_status_update = pd.DataFrame(self.store_list, columns=columns)
        nv_prd_status_update.to_csv(self.path_scrape_df, index=False)
        
        _nv_prd_status_update = nv_prd_status_update[nv_prd_status_update.product_status==1]    # 판매 중 상품
        _nv_prd_status_update_price = _nv_prd_status_update.loc[_nv_prd_status_update.page_status==1]    # 네이버 뷰티윈도 가격비교 탭 상품
        _nv_prd_status_update_all = _nv_prd_status_update.loc[_nv_prd_status_update.page_status==2]    # 네이버 뷰티윈도 전체 탭 상품
        _nv_prd_status_update_dedup = pd.concat([_nv_prd_status_update_price, _nv_prd_status_update_all]).drop_duplicates('item_key', keep='first')
        
        # merge naver info & glowpick info
        df_mer = _nv_prd_status_update_dedup.merge(gl_info, on='item_key', how='left').sort_values(by='item_key', ignore_index=True)
        
        # category 
        df_mer.loc[:, 'category'] = self.categ
        # regist date
        df_mer.loc[:, 'regist_date'] = pd.Timestamp(self.date)
        
        # concat temp table & dup check 
        df_temp = self.db.get_tbl(f'beauty_kr_{self.categ_eng}_info_all_temp')
        if df_temp is None:
            pass
        else:
            df_mer = pd.concat([df_mer, df_temp]).drop_duplicates('item_key', keep='first').sort_values(by='item_key', ignore_index=True)
        
        return status_df_dedup, df_mer
        
    def _upload_df(self, comp=False):
        ''' table upload into db '''
        
        table_name = f'beauty_kr_{self.categ_eng}_info_all'
        try:
            status_df_dedup, df_mer = self._preprocess()
                
            # update status table
            self.db.engine_upload(status_df_dedup, 'naver_beauty_product_info_status', 'replace')
            
            # table upload
            if comp:
                self.db.create_table(df_mer, table_name)    
                os.remove(self.path_input_df)
                os.remove(self.path_store_list)
                os.remove(self.path_status_list)
            else:
                table_name = table_name + '_temp'
                self.db.engine_upload(df_mer, table_name, 'replace')
            status = 1
        
        except Exception as e:
            # db 연결 끊김: 인터넷(와이파이) 재연결 필요
            print(e)
            if self.power:
                self.stop()
            self.check = 2
            status = 0
        
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
                        # 네이버 vpn ip 차단
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
        
tp = TitlePreProcess()
class ThreadCrawlingNvInfo(QtCore.QThread, QtCore.QObject):
    ''' Thread scraping naver product info '''
    
    def __init__(self, parent=None):
        super().__init__()
        self.power = False
        
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = AccessDataBase(conn[0], conn[1], conn[2])
        
        # error log
        self.err = Errors()
        
        # path
        self.crawled_data = os.path.join(tbl_cache, 'crawled_data.csv')
        self.scraping_data = os.path.join(tbl_cache, 'scraping_data.txt')
        self.scraping_data_df = os.path.join(tbl_cache, 'scraping_data.csv')
        
    def get_data(self):
        ''' Get crawled data '''
        
        dash_df = self.db.get_tbl('beauty_kr_data_dashboard', ['item_key', 'mapping_status', 'available_status', 'category'])
        gl_df = self.db.get_tbl('glowpick_product_info_final_version', ['id', 'product_name', 'brand_name']).rename(columns={'id': 'item_key'})
        gl_new_df = self.db.get_tbl('glowpick_product_info_update_new', ['id', 'selection', 'division', 'groups', 'crawling_status']).rename(columns={'id': 'item_key'})
        gl_new_df.loc[:, 'table_name'] = 'glowpick_product_info_final_version'
        gl_new_df_categ = tp.categ_reclassifier(gl_new_df)

        _dash_df = dash_df.loc[(dash_df.mapping_status==1) & (dash_df.available_status==0), ['item_key', 'category']]
        _gl_new_df = gl_new_df_categ.loc[gl_new_df_categ.crawling_status==0, ['item_key', 'category']]

        dash_df_concat = pd.concat([_gl_new_df, _dash_df])
        dash_df_dedup = dash_df_concat.drop_duplicates('item_key', keep='first', ignore_index=True)

        dash_df_mer = dash_df_dedup.loc[:, ['item_key', 'category']].merge(gl_df, on='item_key', how='left')
        
        return dash_df_mer
    
    def _preprocess(self, comp=False):
        ''' Preprocessing scraping data'''
        
        with open(self.scraping_data, 'rb') as f:
            scraping_data = pickle.load(f)

        # assign dataframe        
        columns = ['id','input_words','product_name','product_url','price','category','product_description','registered_date']
        scrape_df = pd.DataFrame(scraping_data, columns=columns)

        if not comp:
            return scrape_df
        else:
            # dup check
            _sorting = ['product_name']
            scrape_df = scrape_df.sort_values(by=_sorting, ascending=False, ignore_index=True)
            subset = ['product_name', 'category']
            df_dedup = scrape_df.drop_duplicates(subset=subset, keep='first', ignore_index=True)

            # mapping brand
            brand_tbl = self.db.get_tbl('beauty_kr_product_brands')

            for idx in tqdm(range(len(df_dedup))):
                brand = df_dedup.loc[idx, 'input_words'].split()[0]
                product_name = df_dedup.loc[idx, 'product_name']
                product_names = product_name.split()
                _brand = product_names[0]
                if brand == _brand:
                    # 브랜드 일치
                    df_dedup.loc[idx, 'brand_name'] = brand
                    status = 1
                else:
                    try:
                        brands = brand_tbl.loc[brand_tbl.keytalk_brand==brand, 'keytalk_data'].values[0]
                        
                        if _brand in brands:
                            # 브랜드 키토크 일치
                            df_dedup.loc[idx, 'brand_name'] = brand
                            status = 1
                        
                        elif brand in product_names:
                            # 브랜드명 종속 (list)
                            df_dedup.loc[idx, 'brand_name'] = brand
                            status = 2
                            
                        elif brand.replace(' ', '') in product_name.replace(' ', ''):
                            # 브랜드명 종속 (str)
                            df_dedup.loc[idx, 'brand_name'] = brand
                            status = 3
                            
                        else:
                            # 브랜드 불일치
                            df_dedup.loc[idx, 'brand_name'] = np.nan
                            status = 0
                            
                    except IndexError:
                        # 브랜드 불일치
                        df_dedup.loc[idx, 'brand_name'] = np.nan
                        status = 0
                df_dedup.loc[idx, 'brand_status'] = status

            # 브랜드 매핑 완료 개체만 추출
            df_dedup = df_dedup[df_dedup.brand_status!=0].reset_index(drop=True)

            # preprocessing columns
            info_detail = pd.DataFrame()
            for idx in tqdm(range(len(df_dedup))):
                # url
                url = df_dedup.loc[idx, 'product_url']
                info_detail.loc[idx, 'product_url'] = url

                # category
                categs = df_dedup.loc[idx, 'category']
                if str(categs) == 'nan':
                    pass
                else:
                    categs = ast.literal_eval(categs)
                    for i in range(len(categs)):
                        info_detail.loc[idx, f'categ_{i}'] = categs[i]
                
                # description
                desc = df_dedup.loc[idx, 'product_description']
                if str(desc) == 'nan':
                    pass
                else:
                    desc = ast.literal_eval(desc)
                    for key in desc.keys():
                        val = desc[key]
                        info_detail.loc[idx, key] = val

            # rename columns
            rename = {
                'categ_0': 'selection',
                'categ_1': 'division',
                'categ_2': 'groups',
                'categ_3': 'gruop_details',
                '용량': 'volume',
                '피부타입': 'skin_type',
                '주요제품특징': 'main_feature',
                '세부제품특징': 'detail_feature',
                'PA지수': 'pa_factor',
                '자외선차단지수': 'sun_protectiom_factor',
                '사용부위': 'usage_area',
                '사용시간': 'usage_time',
                '색상': 'color',
                '타입': 'type',
            }
            columns = ['product_url'] + list(rename.values())
            info_detail_rename = info_detail.rename(columns=rename)

            # find existing columns
            _columns = []
            for col in info_detail_rename.columns:
                if col in columns:
                    _columns.append(col)
            _info_detail_rename = info_detail_rename.loc[:, _columns]

            # merge & sorting
            df_merge = df_dedup.loc[:, ['product_url', 'product_name', 'brand_name']].merge(_info_detail_rename, on='product_url', how='left')
            upload_df = df_merge[(df_merge.brand_name.notnull()) & (df_merge.selection=='화장품/미용')].sort_values(['brand_name', 'division', 'groups'], ignore_index=True)

            return upload_df

    def _upload(self, comp=False):
        ''' Upload data into db '''
        
        df = self._preprocess(comp)
        df.loc[:, 'review_status'] = False
        df.loc[:, 'regist_date'] = pd.Timestamp(datetime.today())
        df.to_csv(self.scraping_data_df, index=False)
        
        if not comp:
            table_name = 'naver_beauty_product_info_final_version_temp'
            self.db.engine_upload(df, table_name, 'replace')
        else:
            table_name = 'naver_beauty_product_info_final_version'
            self.db.create_table(df, table_name, append=True)
        
    progress = QtCore.pyqtSignal(object)
    def run(self):
        ''' 브랜드명 + 상품명으로 상품정보 스크래핑해서 데이터프레임에 할당 '''
        
        crawled_data = pd.read_csv(self.crawled_data)    
            
        if os.path.isfile(self.scraping_data):
            with open(self.scraping_data, 'rb') as f:
                scraping_data = pickle.load(f)
        else:
            scraping_data = []
        
        t = tqdm(range(len(crawled_data)))
        for i in t:
            if self.power:
                # Run: 작업 수행
                self.progress.emit(t)
                
                item_key = crawled_data.loc[i, 'item_key']
                product_name = crawled_data.loc[i, 'product_name']
                brand_name = crawled_data.loc[i, 'brand_name']    
                search_word = brand_name + ' ' + product_name
                try:
                    scrapes, status = crawler_nv(item_key, search_word)
                    scraping_data += scrapes
                except:
                    input_data = search_word.replace('[단종]', '') # '[단종]' 제거 
                    input_txt = re.sub(r'[\(\)\{\}\[\]\/]', ' ', input_data) # bracket 제거
                    input_txt_ = re.sub(r' +', ' ', input_txt).strip() # 다중 공백 제거
                    input_keyword = input_txt_.replace(' ','%20') # 쿼리 내 인터벌
                    url = f'https://search.shopping.naver.com/search/all?&frm=NVSHCAT&origQuery={input_keyword}%20%20%20-세트%20-리필%20-set%20-Set%20-SET%20-패키지%20-페키지%20-Package%20-PACKAGE&pagingIndex=1&pagingSize=40&productSet=model&query={input_keyword}&sort=rel&timestamp=&viewType=list&xq=세트%20리필%20set%20Set%20SET%20패키지%20페키지%20Package%20PACKAGE'
                    self.err.errors_log(url)
            else:
                break
        
        # save cache file
        crawled_data.loc[i:].to_csv(self.crawled_data, index=False)
        with open(self.scraping_data, 'wb') as f:
                pickle.dump(scraping_data, f)
        
        if i == len(crawled_data) - 1:
            self._upload(comp=True)
        else:
            self._upload()
        
        self.progress.emit(t)
        self.power = False
    
    def stop(self):
        ''' Stop Thread '''
        
        self.power = False
        self.quit()
        self.wait(3000)