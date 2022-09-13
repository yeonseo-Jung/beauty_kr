import os
import re
import sys
import time
import numpy as np
import pandas as pd

# Scrapping
from bs4 import BeautifulSoup
from user_agent import generate_user_agent
from selenium.webdriver.common.by import By
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.common.alert import Alert
# from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

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
    
from scraping.scraper import get_url
from hangle import _distance

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
        if wd is None:
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
                    wd.quit()
                    wd = None
                
        return wd, page_status
    
    def get_prd_status(self, wd):
        ''' get product status '''
        
        soup = BeautifulSoup(wd.page_source, 'lxml')
        
        # stp_class = 'noPrice_product_status__2T5PM'
        stp_class = 'noPrice_product_status__WvCuy' # div class 변경됨
        if soup.find('div', stp_class) is not None or soup.find('h3', 'noPrice_status__lBnHb') is not None:
            # 판매중단
            product_status = 0
        elif soup.find('div', 'style_content_error__1XNYB') is not None or soup.find('div', 'error layout_wide theme_trendy') is not None:
            # 상품 존재 x 
            product_status = -1
        elif soup.find('table', 'productByMall_list_seller__yNhgM') is not None:
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
            
            # Price Tab
            if page_status == 1:
                if product_status == 1 or product_status == -2:
                    try:
                        # table_class = 'productByMall_list_seller__2-bzE'
                        table_class = 'productByMall_list_seller__yNhgM' # table tag: class명 변경됨
                        store_table = soup.find('table', table_class).find('tbody')
                        store_list = store_table.find_all('tr')
                    except AttributeError:
                        store_list = []
                        
                    if len(store_list) == 0:
                        stores = None
                    else:
                        for store in store_list:
                            # store name
                            # store_class = 'productByMall_mall__1ITj0'
                            store_class = 'productByMall_mall__SIa50' # a tag: class명 변경됨
                            store_name = store.find('a', store_class).text.strip()
                            if store_name == '':
                                store_name = store.find('img')['alt'].strip()
                            store_names.append(store_name)
                            
                            # store url
                            store_url = store.find('a', store_class)['href']
                            store_urls.append(store_url)
                            
                            # product price
                            price = int(store.find('em').text.replace(',', '').replace(' ', ''))
                            prices.append(price)
                            
                            # delivery_fee
                            # delivery_class = 'productByMall_gift__W92gX'
                            delivery_class = 'productByMall_gift__oidOR' # td tag: class명 변경됨
                            delivery_fee = store.find('td', delivery_class).text.replace(',', '').replace('원', '').replace(' ', '')
                            if delivery_fee == "무료배송":
                                delivery_fee = 0
                            else:
                                try:
                                    delivery_fee = int(delivery_fee)
                                except ValueError:
                                    delivery_fee = np.nan
                            delivery_fees.append(delivery_fee)
                            
                            # naver pay
                            if store.find('span', 'n_npay_icon__DxpI2') is not None:
                                npay = 1
                            elif store.find('span', 'n_ico_npay_plus__1pi8I') is not None:
                                npay = 1
                            elif store.find('span', 'n_icon__1DV3M') is not None:
                                npay = 1
                            else:
                                npay = 0
                            npays.append(npay)
                        stores = [item_key, url, str(store_names), str(store_urls), str(prices), str(delivery_fees), str(npays), int(product_status), int(page_status)]
                else:
                    stores = None
                    
            # All Tab
            elif page_status == 2:
                if product_status == -1:
                    stores = None
                else:
                    if soup.find('a', '_2-uvQuRWK5') is None:
                        product_status = 0
                    else:
                        product_status = 1
                    
                    # store name
                    if soup.find('span', 'KasFrJs3SA') is not None:
                        store_name = soup.find('span', 'KasFrJs3SA').text.strip()
                    elif soup.find('img', '_1QhZSUVBeK') is not None:
                        store_name = soup.find('img', '_1QhZSUVBeK')['alt']
                    else:
                        store_name = np.nan
                    store_names.append(store_name)
                    
                    # store url
                    store_url = wd.current_url
                    store_urls.append(store_url)
                    
                    # product price
                    if len(soup.find_all('span', '_1LY7DqCnwR')) == 0:
                        price = np.nan
                    else:
                        price = int(soup.find_all('span', '_1LY7DqCnwR')[-1].text.replace(',', '').replace(' ', ''))
                    prices.append(price)
                    
                    # delivery_fee
                    if soup.find('span', 'bd_3uare') is None:
                        delivery_fee = 0
                    else:
                        delivery_fee = int(soup.find('span', 'bd_3uare').text.replace(',', '').replace(' ', ''))
                    delivery_fees.append(delivery_fee)
                    
                    # naver pay
                    npay = 1
                    npays.append(npay)
                        
                    stores = [item_key, url, str(store_names), str(store_urls), str(prices), str(delivery_fees), str(npays), int(product_status), int(page_status)]
                        
        return product_status, stores
    
def crawler_nv(product_id, search_word):
    ''' 네이버 뷰티윈도 가격비교탭에서 검색어 입력해서 상품 정보 스크레이핑
    ::Input data::
    - product_id: 상품 식별 id
    - search_word: 검색하려는 상품명 
    
    ::Output data::
    - scraps: 스크레이핑한 상품 정보 리스트
    - status: 스크레이핑 상태 변수 (-1: 아직 스크레이핑 안됨(defalt), 0: 검색결과 없음, 1: 검색결과 존재
    '''
    
    input_data = search_word.replace('[단종]', '') # '[단종]' 제거 
    input_txt = re.sub(r'[\(\)\{\}\[\]\/]', ' ', input_data) # bracket 제거
    input_txt_ = re.sub(r' +', ' ', input_txt).strip() # 다중 공백 제거
    input_keyword = input_txt_.replace(' ','%20') # 쿼리 내 인터벌
    # set up url 
    url = f'https://search.shopping.naver.com/search/all?&frm=NVSHCAT&origQuery={input_keyword}%20%20%20-세트%20-리필%20-set%20-Set%20-SET%20-패키지%20-페키지%20-Package%20-PACKAGE&pagingIndex=1&pagingSize=40&productSet=model&query={input_keyword}&sort=rel&timestamp=&viewType=list&xq=세트%20리필%20set%20Set%20SET%20패키지%20페키지%20Package%20PACKAGE'

    # get url 
    wd = get_url(url)
    if wd == None:
        scraps = []
        status = -1 # status when parsing url fails
    
    else:
        # scroll down
        # scroll_down(wd)
        html = wd.page_source
        soup = BeautifulSoup(html,'lxml') 
        item_divs = soup.find_all('div',class_='basicList_inner__eY_mq')
        scraps = []
        
        if len(item_divs) == 0:
            pass
        
        else:
            cnt = 0
            for item_div in item_divs:

                # product name
                product_name = item_div.find('div',class_='basicList_title__3P9Q7').text 

                # # 문자열 유사도 스코어 확인 (levenshtein distance 활용)
                # # input 값이 title과 50%이상 일치하면 수집 
                # word_0 = input_txt_.replace(' ', '')
                # word_1 = product_name.replace(' ', '')
                # cost = _distance.jamo_levenshtein(word_0, word_1)
                # max_len = max(len(word_0), len(word_1))
                # sim = (max_len - cost) / max_len
                
                if '세트' not in product_name and '기획' not in product_name:
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
                    
                    scraps.append([int(product_id), str(input_txt_), str(product_name), str(product_url), str(price), str(category_), str(product_description), str(registered_date_), int(product_reviews_count), float(product_rating), str(product_store)])
                    cnt += 1
                    
                else:
                    pass
                
                # 최대 상품 5개 까지 수집
                if cnt == 5:
                    break    
        wd.quit()
        
        if len(scraps) == 0:
            status = 0
        else:
            status = 1
            
    return scraps, status
    
class ReviewScrapeNv:    
    
    def parsing(self, driver):
        ''' html parsing '''
        
        html = driver.page_source
        soup = BeautifulSoup(html, 'lxml')
        review_area = soup.find('div', {'class': 'review_section_review__1hTZD'})
        return review_area
    
    def review_scraping(self, driver, rating, review_info, review_text):
        ''' Scraping review data '''
        
        review_soup = self.parsing(driver)
        if review_soup is None:
            status = 0
        else:
            status = 1
            for i in range(len(review_soup.find_all("div", {"class":"reviewItems_etc_area__2P8i3"}))):
                rating.append(int(review_soup.find_all("span", {"class":"reviewItems_average__16Ya-"})[i].text[-1]))
                review_info.append(str([x.text.strip() for x in review_soup.find_all("div", {"class":"reviewItems_etc_area__2P8i3"})[i].find_all("span", {"class":"reviewItems_etc__1YqVF"})]))
                review_text.append(review_soup.find_all("div", {"class":"reviewItems_review__1eF8A"})[i].find("p", {"class":"reviewItems_text__XIsTc"}).text.strip())
            
        return rating, review_info, review_text, status
    
    def click_each_rating(self, driver, i):
        
        rating_tab = driver.find_element_by_css_selector("#section_review > div.filter_sort_group__Y8HA1")
        actions = ActionChains(driver)
        actions.move_to_element(rating_tab).perform()    # scroll to rating tab list to click each rating tab
        time.sleep(1.5)
        driver.find_element_by_xpath(f'//*[@id="section_review"]/div[2]/div[2]/ul/li[{i+2}]/a').click()
        time.sleep(1.5)
        return driver
    
    def pagination(self, driver):
        ''' Scraping reviews as turning pages '''
    
        rating, review_info, review_text = [], [], []
        try:
            element = driver.find_element_by_xpath('//*[@id="section_review"]/div[3]')
            page = BeautifulSoup(element.get_attribute('innerHTML'), 'lxml')
            page_list = page.find_all('a')
            page_num = len(page_list)
            
            rating, review_info, review_text, status = self.review_scraping(driver, rating, review_info, review_text)
            for i in range(2, page_num + 1):
                driver.find_element_by_xpath(f'//*[@id="section_review"]/div[3]/a[{i}]').click()
                time.sleep(1)
                rating, review_info, review_text, status = self.review_scraping(driver, rating, review_info, review_text)
                
            if page_num == 11:
                page_num += 1
            
            # page 10 초과    
            cnt = 1
            break_ck = 0
            while page_num == 12 and break_ck == 0:
                element = driver.find_element_by_xpath('//*[@id="section_review"]/div[3]')
                page = BeautifulSoup(element.get_attribute('innerHTML'), 'lxml')
                page_list = page.find_all('a')
                page_num = len(page_list)
                
                for i in range(3, page_num + 1):
                    if i == 12:
                        cnt += 1
                        
                    # 최대 수집 가능 리뷰 수 2000개 초과시 break (page 100)
                    if cnt == 10:
                        break_ck = 1
                        break
                    else:
                        driver.find_element_by_xpath(f'//*[@id="section_review"]/div[3]/a[{i}]').click()
                        time.sleep(1.5)
                        rating, review_info, review_text, status = self.review_scraping(driver, rating, review_info, review_text)

        except NoSuchElementException:
            # 리뷰 페이지가 한개만 존재할 때
            rating, review_info, review_text, status = self.review_scraping(driver, rating, review_info, review_text)
    
        return rating, review_info, review_text, driver

    def review_crawler(self, url):
        ''' Crawl reviews by rating '''
        
        review_ratings, review_infos, review_texts = [], [], []
        
        driver = get_url(url)
        if driver is None:
            status = -1
            return [np.nan], [np.nan], [np.nan], status
            
        else:
            html = driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # if page does not exist
            if soup.find("div", {"class":"style_content_error__3Wxxj"}) is not None:
                status = -2
                driver.close()
                driver.quit()
                return [np.nan], [np.nan], [np.nan], status

            # if review does not exist 
            elif soup.find("div", {"class":"review_section_review__1hTZD"}) is None:
                status = 0
                driver.close()
                driver.quit()
                return [np.nan], [np.nan], [np.nan], status

            else:
                # # 정렬: 최신순으로 변경 -> 리뷰 업데이트 시에 사용 
                # driver.find_element_by_xpath('//*[@id="section_review"]/div[2]/div[1]/div[1]/a[2]').click() #sort on recent time
                # time.sleep(1)

                ratings = soup.find('ul', 'filter_top_list__3rOdK')
                review_cnt = [int(x.text[1:-1].replace(',', '')) for x in ratings.find_all("em")][1:] #review count for each rating
                
                for i in range(len(review_cnt)): #scrap reviews for each rating by using tablist
                    if review_cnt[i] == 0:
                        pass
                    else:
                        # 평점 선택
                        driver = self.click_each_rating(driver, i)
                        # 리뷰 데이터 스크레이핑
                        review_rating, review_info, review_text, driver = self.pagination(driver)
                        # extend
                        review_ratings.extend(review_rating)
                        review_infos.extend(review_info)
                        review_texts.extend(review_text)
                        
                driver.close()
                driver.quit()
            
                try:
                    if len(review_text) != len(review_rating) or len(review_text) != len(review_info):
                        raise Exception("Review data format error>")                    
                    else:
                        status = 1
                        return review_ratings, review_infos, review_texts, status
                except Exception as e:
                    status = -3
                    return [np.nan], [np.nan], [np.nan], status