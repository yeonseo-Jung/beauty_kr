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


if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    conn_path = os.path.join(base_path, 'conn.txt')
    
else:
    base_path = os.path.dirname(os.path.realpath(__file__))
    tbl_cache = root + '/tbl_cache'
    conn_path = os.path.join(src, 'gui/conn.txt')

def scroll_down(wd):
    prev_height = wd.execute_script("return document.body.scrollHeight")
    
    while True:
        wd.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        wd.implicitly_wait(5)
        time.sleep(1)
        current_height = wd.execute_script("return document.body.scrollHeight")

        if prev_height == current_height:
            break
        prev_height = current_height
        
def get_nv_item_link_by_brd_new(input_data, product_id):
    
    ''' 가격비교 탭 상품 리스트 크롤링 '''
    
    input_data_ = input_data.replace('[단종]', ' ')
    input_txt = re.sub(r'[\(\)\{\}\[\]\/]', ' ', input_data_)
    input_txt_ = re.sub(r' +', ' ', input_txt).strip()
    input_keyword = input_txt_.replace(' ','%20') # 쿼리 내 인터벌
    # 검색 url
    search_result_url = f'https://search.shopping.naver.com/search/all?&frm=NVSHCAT&origQuery={input_keyword}%20%20%20-세트%20-리필%20-set%20-Set%20-SET%20-패키지%20-페키지%20-Package%20-PACKAGE&pagingIndex=1&pagingSize=40&productSet=model&query={input_keyword}&sort=rel&timestamp=&viewType=list&xq=세트%20리필%20set%20Set%20SET%20패키지%20페키지%20Package%20PACKAGE'
    
    socket.setdefaulttimeout(30)
    attempts, ck = 0, 0 # cnt: url 파싱 시도 횟수, ck: url 파싱 성공여부
    while True:
        if attempts == 10:
            # 10번까지 url parsing 시도해보고 안되면 pass 
            print(f"\n\n** URL Parsing Failed **\n{search_result_url}\n\n")
            
            break
        
        try:  
            # user agent
            options = Options() 
            ua = UserAgent(verify_ssl=False, use_cache_server=False, cache=False)
            userAgent = ua.chrome 
            print(userAgent)
            options.add_argument('headless')
            options.add_argument('window-size=1920x1080')
            options.add_argument("disable-gpu")
            options.add_argument(f'user-agent={userAgent}')
                
            # web driver 
            wd = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
            wd.get(search_result_url)
            time.sleep(1)
            ck = 1
            break
        
        except (WebDriverException, Timeout, ReadTimeout, ConnectionError, RequestException, TimeoutException, SSLError, FakeUserAgentError):
            try:
                wd.quit()
            except:
                pass
            time.sleep(30)
            
        except Exception as e:
            try:
                wd.quit()
            except:
                pass
            time.sleep(30)
            print(f'\n{e}\n')
            Error_msg = "Could not find a suitable TLS CA certificate bundle"
            if Error_msg in e:
                certifi_file = e.split(":")[1].strip()
                certifi_dir = "/".join(certifi_file.split("/")[:-1])
                ssl = os.path.join(base_path, 'ssl') 
                file = os.listdir(ssl)[0]
                os.replace(ssl + file, certifi_dir + file)

            
            
        attempts += 1
            
    if ck == 1:   
        scroll_down(wd)
        html = wd.page_source
        soup = BeautifulSoup(html,'lxml') 
        item_divs = soup.find_all('div',class_='basicList_inner__eY_mq')

        scraps = []
        cnt = 0
        for item_div in item_divs:

            # product name
            product_name = item_div.find('div',class_='basicList_title__3P9Q7').text 

            # 문자열 유사도 스코어 확인 (levenshtein distance 활용)
            # input 값이 title과 50%이상 일치하면 수집 
            word_0 = input_txt_.replace(' ', '')
            word_1 = product_name.replace(' ', '')
            cost = _distance.jamo_levenshtein(word_0, word_1)
            max_len = max(len(word_0), len(word_1))
            sim = (max_len - cost) / max_len
            
            if sim >= 0.5 and '세트' not in product_name:

                # product_url
                product_url = item_div.find('a')['href']

                # price
                if item_div.find('span',class_='price_num__2WUXn') != None:
                    price = item_div.find('span',class_='price_num__2WUXn').text 
                else:
                    price = np.nan

                # category
                if item_div.find('div',class_='basicList_depth__2QIie') != None:
                    category = item_div.find('div',class_='basicList_depth__2QIie')
                    category_ = [ctg.text for ctg in category] 

                # product_description
                if item_div.find('div',class_='basicList_detail_box__3ta3h') != None:
                    descriptions = item_div.find('div',class_='basicList_detail_box__3ta3h')
                    if descriptions.find('a', class_='basicList_detail__27Krk') != None:
                        descriptions_ = descriptions.text.split('|')
                        desc_dict = {}
                        for desc in descriptions_:
                            key = desc.split(':')[0].replace(' ', '')
                            value = desc.split(':')[1].replace(' ', '')    
                            desc_dict[key] = value
                        product_description = str(desc_dict)

                    elif descriptions.text != '':
                        desc_dict = {}
                        desc = descriptions.text
                        key = desc.split(':')[0].replace(' ', '')
                        value = desc.split(':')[1].replace(' ', '')    
                        desc_dict[key] = value
                        product_description = str(desc_dict)
                    else:
                        product_description = np.nan

                else:
                    product_description = np.nan

                # registered_date (모든 제품은 등록일을 가지고 있고, 등록일은 동일 클래스 태그 중 맨 항상 맨 앞에 위치)
                if item_div.find('span',class_='basicList_etc__2uAYO') != None: 
                    registered_date = item_div.find('span',class_='basicList_etc__2uAYO').text
                    registered_date_ = registered_date.split('등록일')[-1].rstrip('.')
                else:
                    registered_date_ = np.nan

                # product_reviews_count
                if item_div.find_all('a',class_='basicList_etc__2uAYO') != []:
                    url_boxes = item_div.find_all('a',class_='basicList_etc__2uAYO')
                    url_box = [x for x in url_boxes if '리뷰' in x.text]
                    if len(url_box) != 0:
                        product_reviews_count = url_box[0].find('em',class_='basicList_num__1yXM9').text.replace(',', '')
                    else:
                        product_reviews_count = 0
                else:
                    product_reviews_count = 0

                # product_rating
                if item_div.find('span',class_='basicList_star__3NkBn') != None:
                    product_rating = float(item_div.find('span',class_='basicList_star__3NkBn').text.split('별점')[-1])
                else:
                    product_rating = np.nan

                # product_store
                if item_div.find_all('span',class_='basicList_mall_name__1XaKA') != []:
                    product_store = item_div.find_all('span',class_='basicList_mall_name__1XaKA')[0].text # 최저가 판매처
                else:
                    product_store = np.nan
                
                scraps.append([int(product_id), str(input_txt_), str(product_name), str(product_url), str(price), str(category_), str(product_description), str(registered_date_), int(product_reviews_count), float(product_rating), str(product_store), float(round(sim, 4))])
                cnt += 1
                
            else:
                pass
            
            if cnt == 5:
                break
        
        if len(scraps) == 0:
            status = 0
            print(f"\n\t <Not Found>\n\t{input_txt_}\n")
        else:
            status = 1
        return scraps, status
    
    else:
        return [], -1

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

                outputs = get_nv_item_link_by_brd_new(search_words, id_)
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