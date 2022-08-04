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
from scraping.scraper import get_url, scroll_down
from access_database.access_db import AccessDataBase

def scraper_prd_cnt(category_id, page=1):
    selection = category_id[0:5]
    division = category_id[5:9]
    
    # get url
    url = f'https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=1000001{selection}{division}&fltDispCatNo=&prdSort=01&pageIdx={page}&rowsPerPage=24&searchTypeSort=btn_thumb&plusButtonFlag=N&isLoginCnt=0&aShowCnt=0&bShowCnt=0&cShowCnt=0&trackingCd=Cat1000001{selection}{division}_Small'
    wd = get_url(url)
    if wd is None:
        cnt = -1
    
    else:
        status = 1 # # status when parsing url success
        html = wd.page_source
        soup = BeautifulSoup(html,'lxml')
        time.sleep(1.5)
        
        if soup.find('p', 'cate_info_tx') is None:
            cnt = -1
        else:
            _cnt = soup.find('p', 'cate_info_tx').find('span').text.replace(',', '').strip()
            cnt = int(_cnt)
    return cnt

def scraper_prd_info(category_id, page):
    ''' Scraping product url '''
    
    selection = category_id[0:5]
    division = category_id[5:9]
    scraps = []
    # get url
    url = f'https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=1000001{selection}{division}&fltDispCatNo=&prdSort=01&pageIdx={page}&rowsPerPage=24&searchTypeSort=btn_thumb&plusButtonFlag=N&isLoginCnt=0&aShowCnt=0&bShowCnt=0&cShowCnt=0&trackingCd=Cat1000001{selection}{division}_Small'
    wd = get_url(url)
    if wd is None:
        # status when parsing url fails
        status = -1 
        '''
            status
                * -1: url parsing failed
                *  0: sold out
                *  1: normal
        '''
    else:
        # scroll down
        wd = scroll_down(wd, 1, 3)
        html = wd.page_source
        soup = BeautifulSoup(html,'lxml')
        time.sleep(1.5)
        
        if soup.find('p', 'cate_info_tx').find('span').text.strip() == '0':
            # status when parsing url fails
            status = -1
        
        else:
            # 전체 프로덕트 리스트
            prd_list = soup.find_all('div', 'prd_info')
            if len(prd_list) == 0:
                # status when parsing url fails
                status = -1
            else:
                for prd in prd_list:
                    # 올리브영 프로덕트 코드
                    product_code =prd.find('a', 'prd_thumb goodsList')['data-ref-goodsno'] 

                    # 상품 url
                    url = prd.find('a')['href']

                    # 판매상태
                    if prd.find('span', 'status_flag soldout') is None:
                        status = 1 # 판매중
                    else:
                        status = 0 # 일시품절

                    # 상품명
                    product_name = prd.find('p', 'tx_name').text

                    # 브랜드명
                    brand_name = prd.find('span', 'tx_brand').text

                    # 정상가
                    price = prd.find('span', 'tx_num').text.replace(',', '')

                    # 세일가
                    sale_price = prd.find('span', 'tx_cur').find('span', 'tx_num').text.replace(',', '')

                    # 정보 저장
                    scraps.append((product_code, product_name, url, brand_name, price, sale_price, status))
                    
        wd.quit()
        
    return scraps, status

def scraper_info_detail(url):
    ''' 올리브영 상품 세부정보 스크레이핑 '''
    
    wd = get_url(url)

    if wd is None:
        status = -1
        '''
            status
                * -1: url parsing failed or Product does not exist
                *  0: sold out
                *  1: normal
        '''
        info_list = None

    else:
        html = wd.page_source
        soup = BeautifulSoup(html, 'lxml')
        time.sleep(1.5)
        
        if soup is None:
            status = -1
            info_list = None
            
        elif soup.find('div', 'error-page noProduct') is not None:
            # 상품 존재 x
            status = -1
            info_list = None
        
        else:
            try:
                # 상품명
                product_name = soup.find('p', 'prd_name').text

                # 브랜드명 
                brand_name = soup.find('p', 'prd_brand').text.replace('\n', '')

                # 브랜드 코드
                brand_code = soup.find('div', 'brand_like').find('button')['data-ref-onlbrndcd']

                # 브랜드 링크
                brand_link = f'https://www.oliveyoung.co.kr/store/display/getBrandShopDetail.do?onlBrndCd={brand_code}'

                # 카테고리
                category = soup.find_all('a', 'cate_y')
                if len(category) == 3:
                    selection = category[0].text
                    division = category[1].text
                    groups = category[2].text
                elif len(category) == 2:
                    selection = category[0].text
                    division = category[1].text
                    groups = np.nan
                else:
                    selection = np.nan
                    division = np.nan
                    groups = np.nan

                # 가격
                # 할인가 있을때
                try:
                    price = soup.find('div', 'price').find('span', 'price-1').find('strike').text.replace(',','')
                    sale_price = soup.find('div', 'price').find('span', 'price-2').find('strong').text.replace(',','')

                # 정상가만 있을때
                except AttributeError:  
                    price = soup.find('div', 'price').find('span', 'price-2').find('strong').text.replace(',','')
                    sale_price = None

                # # 리뷰 수 
                # review_cnt = soup.find('div', 'prd_social_info').find('em').text.replace('(', '').replace('건)', '')

                # 리뷰 평점
                product_rating = soup.find('div', 'prd_social_info').find('b').text.replace('\t','').replace('\n', '')

                btn = wd.find_element(By.ID, 'buyInfo')
                wd.implicitly_wait(5)
                try:
                    btn.click()
                except ElementNotInteractableException:
                    ActionChains(wd).move_to_element(btn).click(btn).perform()
                time.sleep(2.5)
                    
                html = wd.page_source
                soup = BeautifulSoup(html, 'lxml')
                
                # 구매 정보 테이블 
                artcInfo = soup.find('div', 'tabConts prd_detail_cont show').find_all('dl', 'detail_info_list')
                
                if len(artcInfo) >= 7:
                    product_size = artcInfo[0].text.split('\n')[2]

                    skin_type = artcInfo[1].text.split('\n')[2]

                    expiration_date = artcInfo[2].text.split('\n')[2]

                    how_to_use = artcInfo[3].text.split('\n')[2]

                    manufacturer = artcInfo[4].text.split('\n')[2]

                    manufactured_country = artcInfo[5].text.split('\n')[2]

                    ingredients = artcInfo[6].text.split('\n')[2]
                else:
                    product_size, skin_type, expiration_date, how_to_use, manufacturer, manufactured_country, ingredients = np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan
                
                if soup.find('button', 'btnSoldout recoPopBtn temprecobell') is None:
                    # normal
                    status = 1
                else:
                    # sold out
                    status = 0
                    
                info_list = [
                url, product_name, brand_name, brand_code, brand_link, selection, division, groups,
                price, sale_price, product_rating, product_size, skin_type, 
                expiration_date, how_to_use, manufacturer, manufactured_country, 
                ingredients, status]
                
            except:
                status = -1
                info_list = None
                    
    return info_list, status, wd

def preprocess_reivew_txt(product_review):
    '''Preprocessing review data(text)'''
    
    product_review = product_review.replace('\xa0', ' ')
    
    if re.sub(r'[^가-힣]+', '', product_review) == '':
        product_review = np.nan
    else:
        review_reg = re.compile(r'[^가-힣a-zA-Z0-9\`\~\!\@\#\$\%\^\&\*\(\)\-\_\=\+\,\<\.\>\/\?\;\:\'\"\[\{\]\}\|]')
        product_review = re.sub(review_reg, ' ', product_review)
        product_review = re.sub(r' +', ' ', product_review)
        
    return product_review

def scraping_review(wd, url):
    '''Scraping review data'''
    
    soup = BeautifulSoup(wd.page_source, 'lxml')
    review_wrap = soup.find('div', 'review_list_wrap')
    review_infos = review_wrap.find_all('div', 'info')
    review_conts = review_wrap.find_all('div', 'review_cont')

    review_data = []
    for info, cont in zip(review_infos, review_conts):
        # user_id
        user_id = info.find('p', 'info_user').find('a', 'id').text

        # product_rating, review_date
        if cont.find('div', 'score_area') is None:
            product_rating = np.nan
            review_date = np.nan
        else:
            if cont.find('div', 'score_area').find('span', 'point') is None:
                product_rating = np.nan
            else:
                product_rating = int(cont.find('div', 'score_area').find('span', 'point').text[-2])
                
            if cont.find('div', 'score_area').find('span', 'date') is None:
                review_date = np.nan
            else:
                try:
                    review_date = pd.Timestamp(cont.find('div', 'score_area').find('span', 'date').text)
                except:
                    review_date = np.nan
                
        # product_review
        if cont.find('div', 'txt_inner') is None:
            product_review = np.nan
        else:
            product_review = cont.find('div', 'txt_inner').text
            product_review = preprocess_reivew_txt(product_review)
            
        review_data.append([url, user_id, product_rating, review_date, product_review])
    
    return review_data

def get_xpath(wd):
    ''' 페이지 구조별로 xpath return '''
    
    soup = BeautifulSoup(wd.page_source, 'lxml')

    a, b = 0, 0
    if soup.find('div', 'prd_free_gift') is None:
        # 증정품 안내 x
        a += 1
        if soup.find('div', 'prd_option_box box_select') is None:
            # 옵션 x
            b += 1
            if soup.find('div', 'poll_all clrfix') is None:
                # 리뷰 분석 x
                b += 1
                if soup.find('div', 'review_thum') is None:
                    # 리뷰 사진 x
                    b += 1
                else:
                    pass
            else:
                if soup.find('div', 'review_thum') is None:
                    # 리뷰 사진 x
                    b += 1
                else:
                    pass

        else:
            if soup.find('div', 'poll_all clrfix') is None:
                # 리뷰 분석 x
                b += 1
                if soup.find('div', 'review_thum') is None:
                    # 리뷰 사진 x
                    b += 1
                else:
                    pass
            else:
                if soup.find('div', 'review_thum') is None:
                    # 리뷰 사진 x
                    b += 1
                else:
                    pass

    else:
        if soup.find('div', 'prd_option_box box_select') is None:
            # 옵션 x
            b += 1
            if soup.find('div', 'poll_all clrfix') is None:
                # 리뷰 분석 x
                b += 1
                if soup.find('div', 'review_thum') is None:
                    # 리뷰 사진 x
                    b += 1
                else:
                    pass
            else:
                if soup.find('div', 'review_thum') is None:
                    # 리뷰 사진 x
                    b += 1
                else:
                    pass

        else:
            if soup.find('div', 'poll_all clrfix') is None:
                # 리뷰 분석 x
                b += 1
                if soup.find('div', 'review_thum') is None:
                    # 리뷰 사진 x
                    b += 1
                else:
                    pass
            else:
                if soup.find('div', 'review_thum') is None:
                    # 리뷰 사진 x
                    b += 1
                else:
                    pass
            
    xpath_filter = f'/html/body/div[3]/div[7]/div/div[{9-a}]/div/div[{4-b}]/div[5]/button' 
    xpath_page = f'/html/body/div[3]/div[7]/div/div[{9-a}]/div/div[{8-b}]'
    # xpath_filter = f'/html/body/div[3]/div[8]/div/div[{9-a}]/div/div[{4-b}]/div[5]/button' 
    # xpath_page = f'/html/body/div[3]/div[8]/div/div[{9-a}]/div/div[{8-b}]'
     
    return xpath_filter, xpath_page

'''Turning Page'''
def turning_page(wd, url, xpath_page):
    # scroll down
    wd.find_element_by_tag_name('body').send_keys(Keys.END)
    time.sleep(2.5)
    
    status = 1
    page_cnt = 1
    init = True
    review_data = []
    while True:
        # scraping page
        soup = BeautifulSoup(wd.page_source, 'lxml')
        page_a = soup.find('div', 'pageing').find_all('a')
        page_n = len(page_a) 

        if page_cnt == 1:
            review_data += scraping_review(wd, url)
            if page_n == 0:
                break
            else:
                start_idx = 1

        else:
            init = False
            if page_n == 1:
                break
            else:
                start_idx = 2
                
        # webdriverwait: turning page
        try:
            WebDriverWait(wd, 10).until(EC.element_to_be_clickable((By.XPATH, xpath_page)))
        except TimeoutException:
            xpath_page = xpath_page[:22] + '8' + xpath_page[23:]
            try:
                WebDriverWait(wd, 10).until(EC.element_to_be_clickable((By.XPATH, xpath_page)))
            except TimeoutException:
                status = -1
                break
        
        for i in range(start_idx, page_n+1):
            btn = wd.find_element_by_xpath(f'{xpath_page}/a[{i}]')
            wd.implicitly_wait(5)
            try:
                btn.click()
            except ElementNotInteractableException:
                ActionChains(wd).move_to_element(btn).click(btn).perform()
                time.sleep(2.5)
            review_data += scraping_review(wd, url)
            page_cnt += 1
                    
        # break condition    
        if init:
            page_n += 1
        else:
            pass
        
        if page_n != 11:
            break
            
    return review_data, status, wd

def select_rating(wd, url, xpath_filter, xpath_page):
    ''' 평점별로 리뷰 수집 '''
    
    status = 1
    review_data = []

    # open review filter
    btn = wd.find_element(By.ID, 'filterBtn')
    wd.implicitly_wait(5)
    try:
        btn.click()
    except ElementNotInteractableException:
        ActionChains(wd).move_to_element(btn).click(btn).perform()
    time.sleep(5)

    # select rating
    try:
        xpath_rating = '/html/body/div[33]/div/div/div[1]/div/div[1]/div/div/dl/dd/ul'
        xpath_apply = '/html/body/div[33]/div/div/div[2]/a[2]'
        WebDriverWait(wd, 10).until(EC.element_to_be_clickable((By.XPATH, xpath_rating)))
    except TimeoutException:
        try:
            xpath_rating = '/html/body/div[34]/div/div/div[1]/div/div[1]/div/div/dl/dd/ul'
            xpath_apply = '/html/body/div[34]/div/div/div[2]/a[2]'
            WebDriverWait(wd, 10).until(EC.element_to_be_clickable((By.XPATH, xpath_rating)))
        except TimeoutException:
            try:
                xpath_rating = '/html/body/div[35]/div/div/div[1]/div/div[1]/div/div/dl/dd/ul'
                xpath_apply = '/html/body/div[35]/div/div/div[2]/a[2]'
                WebDriverWait(wd, 10).until(EC.element_to_be_clickable((By.XPATH, xpath_rating)))
            except TimeoutException:
                status = -1
                
    if status == 1:
        i = 2
        _xpath_rating = f'{xpath_rating}/li[{i}]/label'
        btn = wd.find_element_by_xpath(_xpath_rating)
        wd.implicitly_wait(5)
        try:
            btn.click()
        except ElementNotInteractableException:
            ActionChains(wd).move_to_element(btn).click(btn).perform()
        time.sleep(2.5)

        # apply option
        btn = wd.find_element_by_xpath(xpath_apply)
        wd.implicitly_wait(5)
        try:
            btn.click()
        except ElementNotInteractableException:
            ActionChains(wd).move_to_element(btn).click(btn).perform()
        time.sleep(2.5)

        rev, status, wd = turning_page(wd, url, xpath_page)
        review_data += rev

        # select rating
        # i: 2 ~ 6
        for i in range(3, 7):
            # open review filter
            btn = wd.find_element(By.ID, 'filterBtn')
            wd.implicitly_wait(5)
            try:
                btn.click()
            except ElementNotInteractableException:
                ActionChains(wd).move_to_element(btn).click(btn).perform()
            time.sleep(2.5)

            # select rating
            _xpath_rating = f'{xpath_rating}/li[{i-1}]/label'
            btn = wd.find_element_by_xpath(_xpath_rating)
            wd.implicitly_wait(5)
            try:
                btn.click()
            except ElementNotInteractableException:
                ActionChains(wd).move_to_element(btn).click(btn).perform()
            time.sleep(2.5)
            
            _xpath_rating = f'{xpath_rating}/li[{i}]/label'
            btn = wd.find_element_by_xpath(_xpath_rating)
            wd.implicitly_wait(5)
            try:
                btn.click()
            except ElementNotInteractableException:
                ActionChains(wd).move_to_element(btn).click(btn).perform()
            time.sleep(2.5)

            # apply option
            btn = wd.find_element_by_xpath(xpath_apply)
            wd.implicitly_wait(5)
            try:
                btn.click()
            except ElementNotInteractableException:
                ActionChains(wd).move_to_element(btn).click(btn).perform()
            time.sleep(2.5)

            rev, status, wd = turning_page(wd, url, xpath_page)
            review_data += rev
    else:
        pass

    return review_data, status

def crawling_oliveyoung(url, infos, reviews, error_xpath):
    
    info, status, wd = scraper_info_detail(url)

    if status == -1:
        pass
    else:
        infos.append(info)
        # try:
        soup = BeautifulSoup(wd.page_source, 'lxml')
        # review count
        if soup.find('div', 'prd_social_info') is None:
            cnt = np.nan
        else:
            if soup.find('div', 'prd_social_info').find('em') is None:
                cnt = np.nan
            else: 
                cnt = soup.find('div', 'prd_social_info').find('em').text
                cnt = int(re.sub('[^0-9]', '', cnt))

                if cnt == 0:
                    pass
                else:
                    # click review area
                    btn = wd.find_element(By.ID, 'reviewInfo')
                    wd.implicitly_wait(5)
                    try:
                        btn.click()
                    except ElementNotInteractableException:
                        ActionChains(wd).move_to_element(btn).click(btn).perform()
                    time.sleep(2.5)

                    xpath_filter, xpath_page = get_xpath(wd)

                    if cnt <= 10:
                        # scroll down
                        wd.find_element_by_tag_name('body').send_keys(Keys.END)
                        time.sleep(1.5)
                        reviews += scraping_review(wd, url)
                    elif cnt <= 1000:
                        rev, status, wd = turning_page(wd, url, xpath_page)
                        reviews += rev
                        if status == -1:
                            error_xpath.append(url)
                    else:
                        rev, status = select_rating(wd, url, xpath_filter, xpath_page)
                        reviews += rev
                        if status == -1:
                            error_xpath.append(url)
                            
    return infos, reviews, error_xpath