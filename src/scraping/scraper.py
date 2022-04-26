import os
import re
import sys
import time
import shutil
import certifi
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

# Scrapping
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from selenium import webdriver
# from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Exception Error Handling
import socket
import warnings
warnings.filterwarnings("ignore")

# current directory
cur_dir = os.path.dirname(os.path.realpath(__file__))


if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    conn_path = os.path.join(base_path, 'conn.txt')
    
else:
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src = os.path.abspath(os.path.join(cur_dir, os.pardir))
    sys.path.append(root)
    sys.path.append(src)
    tbl_cache = os.path.join(root, 'tbl_cache')
    conn_path = os.path.join(src, 'gui', 'conn.txt')

# import module inside package
from hangle import _distance

def get_url(url):
    ''' Set up webdriver, useragent & Get url '''
    
    wd = None
    socket.setdefaulttimeout(30)
    attempts = 0 # url parsing 시도횟수
    # 10번 이상 parsing 실패시 pass
    while attempts < 10:
        try:  
            # user agent
            options = Options() 
            ua = UserAgent(verify_ssl=False)
            userAgent = ua.chrome
            print(userAgent)
            options.add_argument('headless')
            options.add_argument('window-size=1920x1080')
            options.add_argument("disable-gpu")
            options.add_argument('--disable-extensions')
            options.add_argument('--blink-settings=imagesEnabled=false')
            options.add_argument(f'user-agent={userAgent}')

            # web driver 
            wd = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
            wd.get(url)
            time.sleep(1.5)
            break

        # 예외처리
        except Exception as e:
            try:
                wd.quit()
            except:
                pass
            print(f'\n\n<Error>\n{e}\n\n')
            time.sleep(30)

            # # TLS CA certificate bundle Error (인증서 업데이트 필요)
            # Error_msg = "Could not find a suitable TLS CA certificate bundle"
            # e = str(e)
            # if Error_msg in e:
            #     # cacert.pem update
            #     _certifi = certifi.where()
            #     dst = "/".join(_certifi.split('/')[:-1])
            #     if os.path.isfile(_certifi):
            #         os.remove(_certifi)
            #         print("\n\nremove: ", _certifi, "\n\n")
            #     os.system('curl -k -O https://curl.haxx.se/ca/cacert.pem')
            #     cacert = 'cacert.pem'
            #     shutil.move(cacert, dst)
        attempts += 1
        
    return wd

def scroll_down(wd):
    ''' 
    Scroll down to the bottom of the page 
    ** 데스크탑 웹 페이지에서만 사용가능 **
    '''
    
    prev_height = wd.execute_script("return document.body.scrollHeight")
    while True:
        wd.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        wd.implicitly_wait(5)
        time.sleep(1)
        current_height = wd.execute_script("return document.body.scrollHeight")

        if prev_height == current_height:
            break
        prev_height = current_height




def scraper_nv(product_id, search_word):
    ''' 네이버 뷰티윈도 가격비교탭에서 검색어 입력해서 상품 정보 스크레이핑 '''
    
    input_data = search_word.replace('[단종]', '') # '[단종]' 제거 
    input_txt = re.sub(r'[\(\)\{\}\[\]\/]', ' ', input_data) # bracket 제거
    input_txt_ = re.sub(r' +', ' ', input_txt).strip() # 다중 스페이스 제거
    input_keyword = input_txt_.replace(' ','%20') # 쿼리 내 인터벌
    # set up url 
    url = f'https://search.shopping.naver.com/search/all?&frm=NVSHCAT&origQuery={input_keyword}%20%20%20-세트%20-리필%20-set%20-Set%20-SET%20-패키지%20-페키지%20-Package%20-PACKAGE&pagingIndex=1&pagingSize=40&productSet=model&query={input_keyword}&sort=rel&timestamp=&viewType=list&xq=세트%20리필%20set%20Set%20SET%20패키지%20페키지%20Package%20PACKAGE'

    # get url 
    wd = get_url(url)
    if wd == None:
        scraps = []
        status = -1 # status when parsing url fails
        pass
    
    else:
        # scroll down
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
            
        wd.quit()
        if len(scraps) == 0:
            status = 0
            print(f"\n\t <Not Found>\n\t{input_txt_}\n")
        else:
            status = 1
            
    return scraps, status
