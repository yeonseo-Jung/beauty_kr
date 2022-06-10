import os
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
                    wd.quit()
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
                            try:
                                delivery_fee = int(delivery_fee)
                            except ValueError:
                                delivery_fee = np.nan
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
        if review_soup == None:
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
        if driver == None:
            status = -1
            return [np.nan], [np.nan], [np.nan], status
            
        else:
            html = driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # if page does not exist
            if soup.find("div", {"class":"style_content_error__3Wxxj"}) != None:
                status = -2
                driver.close()
                driver.quit()
                return [np.nan], [np.nan], [np.nan], status

            # if review does not exist 
            elif soup.find("div", {"class":"review_section_review__1hTZD"}) == None:
                status = 0
                driver.close()
                driver.quit()
                return [np.nan], [np.nan], [np.nan], status

            else:
                # # 최신순으로 변경 -> 리뷰 업데이트 시에 사용 
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