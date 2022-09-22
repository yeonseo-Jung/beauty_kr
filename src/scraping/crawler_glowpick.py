import os
import re
import sys
import time
import pickle
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
    
    def find_division_rank(self, window=None, image=None):
        ''' find division index '''
        
        divisions = {}
        urls, error = [], []
        idx, error_cnt = 1, 0
        while error_cnt < 10:
            url = f"https://www.glowpick.com/categories/{idx}?tab=ranking"
            try:
                wd = get_url(url, window=window, image=image)
                wait_xpath = '/html/body/div/div/div/div/main/div/div[2]/div/div/div[1]/div/div/ul/li[1]'
                WebDriverWait(wd, 30).until(EC.element_to_be_clickable((By.XPATH, wait_xpath)))
                
                # remove popup
                try:
                    popup_xpath = '/html/body/div/div/div/div/div[1]/span/div/div[2]/div[2]/button[1]'
                    wd.find_element(By.XPATH, popup_xpath).click()
                    
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
    
    def find_selection_new(self, window=None, image=None):
        ''' find selection index '''
        selections = {}
        error = []
        idx, error_cnt = 1, 0
        while error_cnt < 10:
            url = f"https://www.glowpick.com/products/brand-new?cate1Id={idx}"
            try:
                # wait for page
                wd = get_url(url, window=window, image=image)
                wait_xpath = '/html/body/div/div/div/div/main/div/div[2]/div/div/div[1]/div/div/ul/li[1]'
                WebDriverWait(wd, 30).until(EC.element_to_be_clickable((By.XPATH, wait_xpath)))
                
                # remove popup
                try:
                    popup_xpath = '/html/body/div/div/div/div/div[1]/span/div/div[2]/div[2]/button[1]'
                    wd.find_element(By.XPATH, popup_xpath).click()
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
                wd.find_element(By.XPATH, popup_xpath).click()
            except NoSuchElementException:
                pass
            
            soup = BeautifulSoup(wd.page_source, 'lxml')
            n = len(soup.find_all('div', 'selector__item__div'))
            urls += self.search_url(soup)
            
            if n >= 2:
                try:
                    groups_xpath = '/html/body/div/div/div/div/div[2]/div/div/div[3]/div/div[2]'
                    wd.find_element(By.XPATH, groups_xpath).click()
                    try:
                        wait_xpath = '/html/body/div/div/div/div/main/div/div[2]/div/div/div[1]/div/div/ul/li[1]'
                        WebDriverWait(wd, 30).until(EC.element_to_be_clickable((By.XPATH, wait_xpath)))
                        link = wd.current_url
                        groups_num = int(re.search(r'ids=[0-9]*', link).group(0).replace('ids=', ''))
                    except (TimeoutException, AttributeError):
                        n = 0
                    
                    while n > 2:
                        # pasing page source
                        soup = BeautifulSoup(wd.page_source, 'lxml')
                        # scraping product url 
                        urls += self.search_url(soup)
                        # next url 
                        link = link.replace(f'ids={groups_num}', f'ids={groups_num+1}')
                        wd.get(link)
                        try:
                            wait_xpath = '/html/body/div/div/div/div/main/div/div[2]/div/div/div[1]/div/div/ul/li[1]'
                            WebDriverWait(wd, 30).until(EC.element_to_be_clickable((By.XPATH, wait_xpath)))
                        except TimeoutException:
                            pass
                        
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
                wd.find_element(By.XPATH, popup_xpath).click()
            except NoSuchElementException:
                pass
            
            soup = BeautifulSoup(wd.page_source, 'lxml')
            n = len(soup.find_all('div', 'selector__item__div'))
            urls += self.search_url(soup)

            if n >= 2:
                try:
                    groups_xpath = '/html/body/div/div/div/div/div[2]/div/div/div[3]/div/div[2]'
                    wd.find_element(By.XPATH, groups_xpath).click()
                    
                    try:
                        wait_xpath = '/html/body/div/div/div/div/main/div/div[2]/div/div/div[1]/div/div/ul/li[1]'
                        WebDriverWait(wd, 30).until(EC.element_to_be_clickable((By.XPATH, wait_xpath)))
                        link = wd.current_url
                        groups_num = int(re.search(r'cate2Id=[0-9]*', link).group(0).replace('cate2Id=', ''))
                    except (TimeoutException, AttributeError):
                        n = 0
                    
                    while n > 2:
                        # pasing page source
                        soup = BeautifulSoup(wd.page_source, 'lxml')
                        
                        # scraping product url 
                        urls += self.search_url(soup)
                        
                        # next url 
                        link = link.replace(f'cate2Id={groups_num}', f'cate2Id={groups_num+1}')
                        wd.get(link)
                        try:
                            wait_xpath = '/html/body/div/div/div/div/main/div/div[2]/div/div/div[1]/div/div/ul/li[1]'
                            WebDriverWait(wd, 30).until(EC.element_to_be_clickable((By.XPATH, wait_xpath)))
                        except TimeoutException:
                            pass
                        
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
            brand_url = re.search(r'https://www.glowpick.com/brands/[0-9]*', brand_code_source)
            if brand_url == None:
                pass
            else:
                brand_url = brand_url.group(0)
                brand_code = int(re.search(r'[0-9]+', brand_url).group(0).strip())

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
                driver.find_element(By.XPATH, open_xpath).click()
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
                
                driver.find_element(By.XPATH, close_xpath).click()
                i += 1
            
            # ingredient    
            open_xpath = f'/html/body/div/div/div/div/main/div/section/div[3]/article[{i}]/h3/button'
            driver.find_element(By.XPATH, open_xpath).click()
            time.sleep(1.5)
            soup = BeautifulSoup(driver.page_source, 'lxml')
            
            if soup.find('li', 'ingredient__list__item item') == None:
                ingredients_all_kor, ingredients_all_eng, ingredients_all_desc = np.nan, np.nan, np.nan
            else:
                ingredients_all_kor, ingredients_all_eng, ingredients_all_desc = [], [], []
                kors = soup.find_all('p', 'item__wrapper__text__kor')
                engs = soup.find_all('p', 'item__wrapper__text__eng')
                descs = soup.find_all('p', 'item__wrapper__text__desc')
                for kor, eng, desc in zip(kors, engs, descs):
                    kor = kor.text.strip()
                    if kor == "":
                        kor = np.nan
                    eng = eng.text.strip()
                    if eng == "":
                        eng = np.nan
                    desc = desc.text.strip()
                    if desc == "":
                        desc = np.nan
                    else:
                        desc = desc.split(',')
                        
                    ingredients_all_kor.append(kor)
                    ingredients_all_eng.append(eng)
                    ingredients_all_desc.append(desc)
                ingredients_all_kor = str(ingredients_all_kor)
                ingredients_all_eng =  str(ingredients_all_eng)
                ingredients_all_desc = str(ingredients_all_desc)
            driver.find_element(By.XPATH, close_xpath).click()
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
            driver.find_element(By.XPATH, open_xpath).click()
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
                desc_keywords = re.findall(reg, desc_keywords)
                if len(desc_keywords) == 0:
                    desc_keywords = np.nan
                else:
                    desc_keywords = str(desc_keywords)
                
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
            driver.find_element(By.XPATH, close_xpath).click()
            
            status = 1
            product_scrapes = [int(product_code), product_name, brand_code, brand_name, url,
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
                
            reviews.append([int(product_code), user_id, product_rating, review_date, product_review])
            
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
                    tag = driver.find_element(By.XPATH, '/html/body/div/div/div/div/main/div/section/section/div[4]')
                    action = ActionChains(driver)
                    action.move_to_element(tag).perform()
                    time.sleep(3)
                    
                    # rating 5 ~ 1 reviews scraping
                    for i in range(4, 9):
                        
                        # click rating button
                        try:
                            rating_xpath = f'/html/body/div/div/div/div/main/div/section/section/div[2]/div[2]/div/div/div[{i}]/span'
                            driver.find_element(By.XPATH, rating_xpath).click()
                            time.sleep(3)
                        except:
                            continue
                        
                        # remove popup
                        try:
                            driver.find_element(By.XPATH, '/html/body/div/div/div/div/div[1]/span/div/div[2]/div/div/div/button[2]').click()
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