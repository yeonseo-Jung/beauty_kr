import os
import re
import sys
import time
import pickle
import numpy as np
import pandas as pd

# Scrapping
from bs4 import BeautifulSoup
from selenium import webdriver
from user_agent import generate_user_agent
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import UnexpectedAlertPresentException, NoAlertPresentException, NoSuchElementException, TimeoutException

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
    
from scraping.scraper import get_url

class ProductStatusNv:
    def __init__(self):
        pass
    
    def _get_url(self, url, window=None, image=None):
        ''' url parsing & get web driver
        
        page_status
            - -1: page parsing failed
            -  1: price tab
            -  2: all tab
        '''
        wd = get_url(url, window=window, image=image)
        time.sleep(2.5)
        if wd == None:
            page_status = -1
            
        else:
            # all tab discrimination
            cur_url = wd.current_url
            if ('smartstore' in cur_url) | ('brand.naver.com' in cur_url):
                page_status = 2
            else:
                try:
                    wait_xpath = '/html/body/div/div/div[1]/div/div[3]'
                    WebDriverWait(wd, 30).until(EC.element_to_be_clickable((By.XPATH, wait_xpath)))
                    page_status = 1
                    
                except TimeoutException:
                    page_status = -1
                    wd = None
                
        return wd, page_status
    
    def get_prd_status(self, wd):
        ''' get product status '''
        
        soup = BeautifulSoup(wd.page_source, 'lxml')
        if soup.find('div', 'noPrice_product_status__2T5PM') != None:
            # 판매중단
            product_status = 0
        elif soup.find('div', 'style_content_error__1XNYB') != None or soup.find('div', 'error layout_wide theme_trendy') != None:
            # 상품 존재 x 
            product_status = -1
        elif soup.find('table', 'productByMall_list_seller__2-bzE') != None:
            # 판매중
            product_status = 1
        else:
            # 모름 
            product_status = -2
            
        return product_status, soup
            
    def scraping_product_stores(self, item_key, url, window, image):
        ''' scraping product stores price tab '''
        
        wd, page_status = self._get_url(url, window, image)
        
        if page_status == -1:
            # page parsing failed
            product_status = -1
            stores = None
        else:
            product_status, soup = self.get_prd_status(wd)
            store_names, store_urls, prices, delivery_fees, npays = [], [], [], [], []
            if page_status == 1:
                if product_status == 1:
                    # Price Tab
                    store_table = soup.find('table', 'productByMall_list_seller__2-bzE').find('tbody')
                    store_list = store_table.find_all('tr')
                    for store in store_list:
                        # store name
                        store_name = store.find('a', 'productByMall_mall__1ITj0').text.strip()
                        if store_name == '':
                            store_name = store.find('img')['alt'].strip()
                        store_names.append(store_name)
                        
                        # store url
                        store_url = store.find('a', 'productByMall_mall__1ITj0')['href']
                        store_urls.append(store_url)
                        
                        # product price
                        price = int(store.find('em').text.replace(',', '').replace(' ', ''))
                        prices.append(price)
                        
                        # delivery_fee
                        delivery_fee = store.find('td', 'productByMall_gift__W92gX').text.replace(',', '').replace('원', '').replace(' ', '')
                        if delivery_fee == "무료배송":
                            delivery_fee = 0
                        else:
                            delivery_fee = int(delivery_fee)
                        delivery_fees.append(delivery_fee)
                        
                        # naver pay
                        if store.find('span', 'n_ico_npay_plus__1pi8I') != None:
                            npay = 1
                        else:
                            npay = 0
                        npays.append(npay)
                        
                    stores = [item_key, str(store_names), str(store_urls), str(prices), str(delivery_fees), str(npays), int(product_status), int(page_status)]
                else:
                    stores = None
                    
            elif page_status == 2:
                if product_status == -1:
                    stores = None
                else:
                    # All Tab
                    if soup.find('a', '_2-uvQuRWK5') == None:
                        product_status = 0
                    else:
                        product_status = 1
                    
                    # store name
                    if soup.find('span', 'KasFrJs3SA') != None:
                        store_name = soup.find('span', 'KasFrJs3SA').text.strip()
                    elif soup.find('img', '_1QhZSUVBeK') != None:
                        store_name = soup.find('img', '_1QhZSUVBeK')['alt']
                    store_names.append(store_name)
                    
                    # store url
                    store_url = wd.current_url
                    store_urls.append(store_url)
                    
                    # product price
                    price = int(soup.find_all('span', '_1LY7DqCnwR')[-1].text.replace(',', '').replace(' ', ''))
                    prices.append(price)
                    
                    # delivery_fee
                    if soup.find('span', 'bd_3uare') == None:
                        delivery_fee = 0
                    else:
                        delivery_fee = int(soup.find('span', 'bd_3uare').text.replace(',', '').replace(' ', ''))
                    delivery_fees.append(delivery_fee)
                    
                    # naver pay
                    npay = 1
                    npays.append(npay)
                        
                    stores = [item_key, str(store_names), str(store_urls), str(prices), str(delivery_fees), str(npays), int(product_status), int(page_status)]
                        
        return product_status, stores