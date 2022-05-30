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

# import module inside package
try:
    from hangle import _distance
except:
    pass

def get_url(url, window=None, image=None):
    ''' Set up webdriver, useragent & Get url '''
    
    wd = None
    socket.setdefaulttimeout(30)
    error = []
    attempts = 0 # url parsing 시도횟수
    # 10번 이상 parsing 실패시 pass
    while attempts < 10:
        try:  
            attempts += 1
            # user agent
            options = Options() 
            userAgent = generate_user_agent(os=('mac', 'linux'), navigator='chrome', device_type='desktop')
            options.add_argument('window-size=1920x1080')
            options.add_argument("--disable-gpu")
            options.add_argument('--disable-extensions')
            if window == None:
                options.add_argument('headless')
            if image == None:
                options.add_argument('--blink-settings=imagesEnabled=false')
            options.add_argument(f'user-agent={userAgent}')

            # web driver 
            wd = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
            wd.get(url)
            wd.implicitly_wait(5)
            break

        # 예외처리
        except Exception as e:
            print(f'\n\nError: {str(e)}\n\n')
            time.sleep(300)
            try:
                wd.quit()
            except:
                pass
            error.append([url, str(e)])
            # try:
            #     with open(f'{tbl_cache}/scraping_error_msg.txt', 'wb') as f:
            #         pickle.dump(error, f)
            # except:
            #     with open('./scraping_error_msg.txt', 'wb') as f:
            #         pickle.dump(error, f)
            wd = None
    return wd
    
def scroll_down(wd):
    ''' 
    Scroll down to the bottom of the page 
    ** 데스크탑 웹 페이지에서만 사용가능 **
    '''
    
    prev_height = wd.execute_script("return document.body.scrollHeight")
    while True:
        wd.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        # wd.implicitly_wait(5)
        time.sleep(1)
        current_height = wd.execute_script("return document.body.scrollHeight")

        if prev_height == current_height:
            break
        prev_height = current_height

def scraper_nv(product_id, search_word):
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

                # 문자열 유사도 스코어 확인 (levenshtein distance 활용)
                # input 값이 title과 50%이상 일치하면 수집 
                word_0 = input_txt_.replace(' ', '')
                word_1 = product_name.replace(' ', '')
                cost = _distance.jamo_levenshtein(word_0, word_1)
                max_len = max(len(word_0), len(word_1))
                sim = (max_len - cost) / max_len
                
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
                    
                    scraps.append([int(product_id), str(input_txt_), str(product_name), str(product_url), str(price), str(category_), str(product_description), str(registered_date_), int(product_reviews_count), float(product_rating), str(product_store), float(round(sim, 4))])
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

class CrawlInfoRevGl():
    def __init__(self):
        pass
        
    def get_webdriver_gl(self, product_code):
        ''' Get WebDriver for glowpick products '''
        
        url = f'https://www.glowpick.com/products/{product_code}'
        status, cnt = 0, 0
        '''
        status
            -2: something else (re-crawling)
            -1: Glowpick blocks VPM ip: Restart VPM (Thread Stop -> VPM reconnect -> re-Run)
            0: parsing failed (re-crawling)
            1: Product exists
            404: Product does not exist
        '''
        while (status != 1) & (status != 404) & (cnt < 5):
            driver = get_url(url=url, window=None, image=1)
            
            if driver == None:
                status = -1
            
            else:
                try:    
                    # Check if Product does not exist (glowpick products)
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div/div/div/div/div/h1')))
                    soup = BeautifulSoup(driver.page_source, 'lxml')
                    if soup.find('div', 'error-page__contents error-page__404') != None:
                        # Product does not exist
                        driver.quit()
                        driver = None
                        status = 404
                    elif soup.find('div', 'error-page__contents error-page__other') != None:
                        # Glowpick blocks VPM ip: Restart VPM
                        driver.quit()
                        driver = None
                        status = -1
                    else:    
                        # something else
                        driver.quit()
                        driver = None
                        status = -2
                    
                except TimeoutException:
                    try:
                        # Wait for page parsing to complete
                        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="contents"]/section/div[2]/p[1]/button')))
                        soup = BeautifulSoup(driver.page_source, 'lxml')
                        if soup.find('div', 'error-page__contents error-page__404') == None:
                            # Product exists
                            status = 1
                        else:
                            # Product does not exist
                            driver.quit()
                            driver = None
                            status = 404
            
                    except TimeoutException:
                        # url parsing failed
                        driver.quit()
                        driver = None
            cnt += 1
        return driver, status
        
    def search_url(self, soup):
        urls = []
        for a in soup.find_all('script'):
            url = re.search(r'https://www.glowpick.com/products/[0-9]*', str(a))
            if url == None:
                pass
            else:
                urls.append(url.group(0))
                
        return urls
    
    def find_division_rank(self):
        ''' find division index '''
        
        divisions = {}
        urls, error = [], []
        idx, error_cnt = 1, 0
        while error_cnt < 10:
            url = f"https://www.glowpick.com/categories/{idx}?tab=ranking"
            try:
                wd = get_url(url)
                wait_xpath = '/html/body/div/div/div/div/main/div/div[2]/div/div/div[1]/div/div/ul/li[1]'
                WebDriverWait(wd, 30).until(EC.element_to_be_clickable((By.XPATH, wait_xpath)))
                
                # remove popup
                try:
                    popup_xpath = '/html/body/div/div/div/div/div[1]/span/div/div[2]/div[2]/button[1]'
                    wd.find_element_by_xpath(popup_xpath).click()
                except NoSuchElementException:
                    pass
                
                # find division index
                soup = BeautifulSoup(wd.page_source, 'lxml')
                division = soup.find('h1', 'nav__h1').find('span').text
                divisions[division] = idx
                
            except Exception as e:
                error_cnt += 1
                error.append([url, str(e)])
            wd.quit()
            idx += 1
            
        return divisions
    
    def find_selection_new(self):
        ''' find selection index'''
        selections = {}
        error = []
        idx, error_cnt = 1, 0
        while error_cnt < 10:
            url = f"https://www.glowpick.com/products/brand-new?cate1Id={idx}"
            try:
                # wait for page
                wd = get_url(url)
                wait_xpath = '/html/body/div/div/div/div/main/div/div[2]/div/div/div[1]/div/div/ul/li[1]'
                WebDriverWait(wd, 30).until(EC.element_to_be_clickable((By.XPATH, wait_xpath)))
                
                # remove popup
                try:
                    popup_xpath = '/html/body/div/div/div/div/div[1]/span/div/div[2]/div[2]/button[1]'
                    wd.find_element_by_xpath(popup_xpath).click()
                except NoSuchElementException:
                    pass
                
                # find division
                soup = BeautifulSoup(wd.page_source, 'lxml')
                selection = soup.find('div', 'selector__item__div').find('span').text.strip()
                selections[selection] = idx
                
            except Exception as e:
                error_cnt += 1
                error.append([url, str(e)])
            wd.quit()
            idx += 1
            
        return selections
    
    def scraping_prds_rank(self, wd):
        
        urls = []
        try:
            wait_xpath = '/html/body/div/div/div/div/main/div/div[2]/div/div/div[1]/div/div/ul/li[1]'
            WebDriverWait(wd, 30).until(EC.element_to_be_clickable((By.XPATH, wait_xpath)))
                        
            # remove popup
            try:
                popup_xpath = '/html/body/div/div/div/div/div[1]/span/div/div[2]/div[2]/button[1]'
                wd.find_element_by_xpath(popup_xpath).click()
            except NoSuchElementException:
                pass
            
            soup = BeautifulSoup(wd.page_source, 'lxml')
            n = len(soup.find_all('div', 'selector__item__div'))
            urls += self.search_url(soup)
            
            if n >= 2:
                try:
                    groups_xpath = '/html/body/div/div/div/div/div[2]/div/div/div[3]/div/div[2]'
                    wd.find_element_by_xpath(groups_xpath).click()
                    # btn = wd.find_element_by_xpath(groups_xpath)
                    # wd.execute_script("arguments[0].click();", btn)
                    time.sleep(3)
                    link = wd.current_url
                    groups_num = int(re.search(r'ids=[0-9]*', link).group(0).replace('ids=', ''))
                    
                    while n > 2:
                        # pasing page source
                        soup = BeautifulSoup(wd.page_source, 'lxml')
                        # scraping product url 
                        urls += self.search_url(soup)
                        # next url 
                        link = link.replace(f'ids={groups_num}', f'ids={groups_num+1}')
                        wd.get(link)
                        time.sleep(5)
                        
                        groups_num += 1
                        n -= 1
                        
                except NoSuchElementException:
                    pass
            urls = list(set(urls))
        except TimeoutException:
            pass
        return urls
    
    def scraping_prds_new(self, wd):
        
        urls = []
        try:
            wait_xpath = '/html/body/div/div/div/div/main/div/div[2]/div/div/div[1]/div/div/ul/li[1]'
            WebDriverWait(wd, 30).until(EC.element_to_be_clickable((By.XPATH, wait_xpath)))
            
            # remove popup
            try:
                popup_xpath = '/html/body/div/div/div/div/div[1]/span/div/div[2]/div[2]/button[1]'
                wd.find_element_by_xpath(popup_xpath).click()
            except NoSuchElementException:
                pass
            
            soup = BeautifulSoup(wd.page_source, 'lxml')
            n = len(soup.find_all('div', 'selector__item__div'))
            urls += self.search_url(soup)

            if n >= 2:
                try:
                    groups_xpath = '/html/body/div/div/div/div/div[2]/div/div/div[3]/div/div[2]'
                    wd.find_element_by_xpath(groups_xpath).click()
                    time.sleep(5)
                    link = wd.current_url
                    groups_num = int(re.search(r'cate2Id=[0-9]*', link).group(0).replace('cate2Id=', ''))
                    
                    while n > 2:
                        # pasing page source
                        soup = BeautifulSoup(wd.page_source, 'lxml')
                        
                        # scraping product url 
                        urls += self.search_url(soup)
                        
                        # next url 
                        link = link.replace(f'cate2Id={groups_num}', f'cate2Id={groups_num+1}')
                        wd.get(link)
                        time.sleep(5)
                        
                        groups_num += 1
                        n -= 1
                        
                except NoSuchElementException:
                    pass
            urls = list(set(urls))
        except TimeoutException:
            pass
        return urls
    
    def scrape_gl_info(self, product_code, driver, review_check):
        ''' glowpick product info detail scraping '''    
        
        url = f'https://www.glowpick.com/products/{product_code}'
        
        if driver == None:
            # page parsing failed
            product_scrapes = np.nan
            status = -1
            
        else: 
            soup = BeautifulSoup(driver.page_source, 'lxml')
            
            # brand
            brand_name = soup.find('button', 'product__summary__brand__name').text.strip()
            brand_code_source = soup.find_all('script',  type="application/ld+json")[-1].text
            brand_url = re.search(r'https://www.glowpick.com/brands/[0-9]*', brand_code_source).group(0)
            brand_code = re.search(r'[0-9]+', brand_url).group(0).strip()

            # product_name
            product_name = soup.find('p', 'product__summary__name').text.strip()

            close_xpath = '/html/body/div/div/div/div/div[1]/span/div/div[2]/h1/button'
            i = 1
            # ranking
            if soup.find('article', 'info__article rank-pd') == None:
                rank_dict = np.nan
                pass
            else:
                rank_dict = {}
                ranks = soup.find_all('li', 'info__article__ul__li rank-item')
                for rank in ranks:
                    rank_name = rank.find('span', 'rank-item__name').text.strip()
                    ranking = rank.find('span', 'rank-item__rank').text.strip()
                    rank_dict[rank_name] = ranking
                rank_dict = str(rank_dict)
                i += 1
                
            # awards
            if soup.find('article', 'info__article award') == None:
                product_awards = np.nan
                product_awards_sector = np.nan 
                product_awards_rank = np.nan
                pass
            else:
                open_xpath = f'/html/body/div/div/div/div/main/div/section/div[3]/article[{i}]/h3/button'
                driver.find_element_by_xpath(open_xpath).click()
                time.sleep(1.5)
                soup = BeautifulSoup(driver.page_source, 'lxml')
                
                product_awards, product_awards_sector, product_awards_rank = [], [], []
                awards = soup.find_all('p', 'awards__item__text__name')
                awards_sector = soup.find_all('span', 'awards__item__text__award')
                awards_rank = soup.find_all('span', 'awards__item__text__rank')
                for award, sector, rank in zip(awards, awards_sector, awards_rank):
                    product_awards.append(award.text.strip())
                    product_awards_sector.append(sector.text.strip())
                    product_awards_rank.append(rank.text.strip())
                product_awards = str(product_awards)
                product_awards_sector =  str(product_awards_sector)
                product_awards_rank = str(product_awards_rank)
                
                driver.find_element_by_xpath(close_xpath).click()
                i += 1
            
            # ingredient    
            open_xpath = f'/html/body/div/div/div/div/main/div/section/div[3]/article[{i}]/h3/button'
            driver.find_element_by_xpath(open_xpath).click()
            time.sleep(1.5)
            soup = BeautifulSoup(driver.page_source, 'lxml')
            
            if soup.find('ul', 'ingredient__list__item item') == None:
                ingredients_all_kor, ingredients_all_eng, ingredients_all_desc = np.nan, np.nan, np.nan
            else:
                ingredients_all_kor, ingredients_all_eng, ingredients_all_desc = [], [], []
                kors = soup.find_all('p', 'item__wrapper__text__kor')
                engs = soup.find_all('p', 'item__wrapper__text__eng')
                descs = soup.find_all('p', 'item__wrapper__text__desc')
                for kor, eng, desc in zip(kors, engs, descs):
                    ingredients_all_kor.append(kor.text.strip())
                    ingredients_all_eng.append(eng.text.strip())
                    ingredients_all_desc.append(desc.text.strip().split(','))
                ingredients_all_kor = str(ingredients_all_kor)
                ingredients_all_eng =  str(ingredients_all_eng)
                ingredients_all_desc = str(ingredients_all_desc)
            driver.find_element_by_xpath(close_xpath).click()
            i += 1
                
            # image source
            soup = BeautifulSoup(driver.page_source, 'lxml')
            if soup.find('div', 'product__image-wrapper') == None:
                img_src = np.nan
            else:
                try:
                    img_src = soup.find('div', 'product__image-wrapper').find('img', 'image__img')['src']
                except:
                    img_src = np.nan
                    
            # descriptions
            open_xpath = f'/html/body/div/div/div/div/main/div/section/div[3]/article[{i}]/h3/button'
            driver.find_element_by_xpath(open_xpath).click()
            time.sleep(1.5)
            soup = BeautifulSoup(driver.page_source, 'lxml')

            # product pre descriptions
            if soup.find('pre', 'descriptions__article__pre') == None:
                desc_pre = np.nan
            else:
                desc_pre = soup.find('pre', 'descriptions__article__pre').text.strip()
                if desc_pre == '-':
                    desc_pre = np.nan
                else:
                    desc_pre = re.sub('[\n\t\r]+', ' ', desc_pre)
                    desc_pre = re.sub(' +', ' ', desc_pre).strip()
            
            # product keywords
            if soup.find('p', 'descriptions__article__keywords') == None:
                desc_keywords = np.nan
            else:
                desc_keywords = soup.find('p', 'descriptions__article__keywords').text.strip()
                reg = re.compile('#[가-힣]+')
                desc_keywords = str(re.findall(reg, desc_keywords))
                
            # color | type
            if '컬러/타입' in str(soup.select('.descriptions__article')):
                color_type = soup.select('.descriptions__article')[1].find('pre').get_text().replace(' ', '')
                color_type = str(color_type.split('/'))
            else:
                color_type = np.nan
                
            # volume & price
            if soup.find('p', 'font-spoqa') == None:
                volume = np.nan
                price = np.nan
            else:
                vol_price = soup.find('p', 'font-spoqa').text.replace(' ', '').split('/')
                volume = vol_price[0].replace('\n', '')
                price = vol_price[1].replace('\n', '')
                if price == '가격미정':
                    price = np.nan
            
            # categories
            selection = np.nan
            division = np.nan
            groups = np.nan
            if soup.find('span', 'descriptions__article__category') != None:
                selection = soup.find('span', 'descriptions__article__category').text.strip()
            categs = soup.find_all('span', 'descriptions__article__category descriptions__article__category-link')
            i = 0
            for categ in categs:
                if i == 0:
                    division = categ.text.strip()
                elif i == 1:
                    groups = categ.text.strip()
                i += 1
                
            stores = soup.find_all('p', 'stores__store__name')
            if len(stores) == 0:
                _stores = np.nan
            else:    
                _stores = []
                for store in stores:
                    _stores.append(store.text.strip())
                _stores = str(_stores)
            driver.find_element_by_xpath(close_xpath).click()
            
            status = 1
            product_scrapes = [product_code, product_name, brand_code, brand_name, url,
                            selection, division, groups, 
                            desc_pre, desc_keywords, color_type, volume, img_src, 
                            ingredients_all_kor, ingredients_all_eng, ingredients_all_desc,
                            rank_dict, product_awards, product_awards_sector, product_awards_rank,
                            price, _stores]
            
        if review_check == 0:
            driver.quit()
            return product_scrapes, status, None
        elif review_check == 1:
            return product_scrapes, status, driver
    
    def scraping_review(self, product_code, driver, soup, reviews):
        ''' Review Data Scraper '''
        
        soup = BeautifulSoup(driver.page_source, 'lxml')
        wrapper = soup.find_all('article', 'review reviews__wrapper__review')
        for rev in wrapper:
            user_id = rev.find('p', 'info__details__nickname').text.strip()
            if user_id == '':
                user_id = np.nan
                
            product_rating = rev.find('span', 'stars__rating font-spoqa').text.replace('\n', '').replace(' ', '')
            if product_rating == '':
                product_rating = np.nan
                
            review_date = rev.find('span', 'review__side-info__created-at').text.replace('\n', '').replace(' ', '')
            if review_date == '':
                review_date = np.nan
                
            product_review = rev.find('pre', 'cutter__pre').text.replace('\n', ' ')
            if product_review == '':
                product_review = np.nan
            else:
                product_review = re.sub(r' +', ' ', product_review).strip()
                
            reviews.append([product_code, user_id, product_rating, review_date, product_review])
            
        return driver, reviews
    
    def crawling_review(self, product_code, driver):
        
        reviews = []
        soup = None
        status = 1
        try:
            xpath = '/html/body/div/div/div/div/main/div/section/section/div[2]/div[2]/div/div/div[4]/span'
            WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, xpath)))
            soup = BeautifulSoup(driver.page_source, 'lxml')
            
            if soup != None:
                count = int(soup.find('span', 'reviews__header__count').text.replace(',', '').strip())
                if count == 0:
                    status = 0
                elif count <= 50:
                    driver, reviews = self.scraping_review(product_code, driver, soup, reviews)
                    if len(reviews) == 0:
                        # parsing error
                        status = -1
                else:
                    # scroll down to select rating 
                    tag = driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/section/div[4]')
                    action = ActionChains(driver)
                    action.move_to_element(tag).perform()
                    time.sleep(3)
                    
                    # rating 5 ~ 1 reviews scraping
                    for i in range(4, 9):
                        
                        # click rating button
                        try:
                            rating_xpath = f'/html/body/div/div/div/div/main/div/section/section/div[2]/div[2]/div/div/div[{i}]/span'
                            driver.find_element_by_xpath(rating_xpath).click()
                            time.sleep(3)
                        except:
                            continue
                        
                        # remove popup
                        try:
                            driver.find_element_by_xpath('/html/body/div/div/div/div/div[1]/span/div/div[2]/div/div/div/button[2]').click()
                            time.sleep(3)
                        except:
                            pass
                        # scraping review data
                        driver, reviews = self.scraping_review(product_code, driver, soup, reviews)
                    
                    if len(reviews) == 0:
                        # parsing error
                        status = -1
                        
            else:
                # parsing error
                status = -1
            
        except TimeoutException:
            # parsing error
            status = -1
        
        driver.quit()
        return reviews, status
    
    
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
        time.sleep(1)
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