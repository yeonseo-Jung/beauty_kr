import os
import re
import sys
import pickle
import time
import numpy as np
import pandas as pd
from tqdm.autonotebook import tqdm

# Scrapping
from bs4 import BeautifulSoup
from selenium import webdriver
from user_agent import generate_user_agent
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementNotInteractableException

# Exception Error Handling
import socket
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

# import module inside package
from scraping.scraper import get_url
from access_database.access_db import AccessDataBase

def scrolling(driver):
    curr_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        # 끝까지 스크롤 다운
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        time.sleep(3)

        # 스크롤 다운 후 스크롤 높이 다시 가져옴
        new_height = driver.execute_script("return document.body.scrollHeight")

        if new_height == curr_height:
            time.sleep(3)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == curr_height:
                print('done scrolling')
                break
            else:
                pass
        curr_height = new_height
    
    return driver
        
def scraping(soup, sub_categ):
    product_list = []
    prd_list=soup.find('div', 'prd-list').find_all('li')
    for prd in prd_list:
        # 브랜드명
        brand_name=prd.find('span', 'bottom-area').find('span', 'category').text.replace('[', '').replace(']', '')
        # 상품명
        prd_name = prd.find('span', 'bottom-area').find('span', 'prd-name').text
        
        # 가격 정보
        prices = prd.find('span', 'bottom-area').find('span', 'price').text.split('원')
        # 세일 안함
        if len(prices) == 2:
            price = prices[0].replace(',', '')
            sale_price = None
        # 세일 함
        if len(prices) == 3:
            price = prices[0].replace(',', '')
            sale_price = prices[1].replace(',', '')
        
        # 상품 코드 
        product_code = prd.find('a')['href'].replace("javascript:hnbNavigation.changeUrlProductDetail('", '').replace("');", '')
        
        # 상품 링크
        product_link = f'https://m.lalavla.com/service/products/productDetail.html?prdId={product_code}'
        
        product_list.append((brand_name, prd_name, price, sale_price, product_code, product_link, sub_categ))
        
    return product_list
        
          
# url = 'https://m.lalavla.com/service/products/productCategory.html?CTG_ID=C090300&CTG_NM=%EA%B8%B0%EC%B4%88%EC%8A%A4%ED%82%A8%EC%BC%80%EC%96%B4'

def crawling(url):
    driver = get_url(url)
    soup = BeautifulSoup(driver.page_source, 'lxml')
    sub_categories = len(soup.find('ul', 'swiper-wrapper cateScroll'))
    product_list = []
    if sub_categories == 0:
        driver = scrolling(driver)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        product_list += scraping(soup, np.nan)
    else:
        for sub_category in range(2, int(sub_categories)+1):
            driver.find_element(By.XPATH, f'/html/body/section/div[1]/div/div[1]/ul/li[{sub_category}]/a').click()
            driver = scrolling(driver)
            soup = BeautifulSoup(driver.page_source, 'lxml')
            sub_categ = soup.find('ul', 'swiper-wrapper cateScroll').find_all('a')[int(sub_category)-1].text
            product_list += scraping(soup, sub_categ)
    return product_list

        
# product_df = pd.DataFrame(product_list, columns = ['brand_name', 'product_name', 'price', 'sale_price', 
#                                                   'product_code', 'product_url', 'sub_category'])