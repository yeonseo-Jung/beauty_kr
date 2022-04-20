from bs4 import BeautifulSoup
import sys
#import dload
import requests as req
import pandas as pd
import requests
from tqdm import tqdm, tqdm_notebook
import re
import time
import numpy as np
import pymysql
from sqlalchemy import create_engine
import os 
import cv2
import selenium
import json
from selenium import webdriver
import pickle
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager  # Chrome
from fake_useragent import UserAgent


import socket
import urllib.request
from urllib.request import urlopen
from urllib.parse import quote_plus
from urllib.request import urlretrieve
from urllib.error import HTTPError, URLError
from selenium.common.exceptions import ElementClickInterceptedException, NoSuchElementException, ElementNotInteractableException

from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC

import os
import requests
import cv2
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import warnings
warnings.filterwarnings("ignore")


def glw_product_detail_info_crw(product_code) :

    #셀레니움 세팅
    options = Options()
    ua = UserAgent()
    userAgent = ua.random
    print(userAgent)

    options.add_argument('headless')
    options.add_argument('window-size=1920x1080')
    options.add_argument("disable-gpu")
    options.add_argument(f'user-agent={userAgent}')
    driver=webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
    socket.setdefaulttimeout(30)


    prod_idx = product_code
    # 모든 제품의 브랜드 이름 모아놓을 리스트
    #all_brands = []  

    # 테이블에서 링크 가져오기
    url = f'https://www.glowpick.com/products/{prod_idx}'
    # 링크로 접속
    driver.get(url)

    # 링크에서 뜨는 팝업 제거 ("오늘 하루 안보기" 클릭하도록 세팅 )
    #remove_button = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="default-layout"]/div/div[1]/span/div/div[2]/div[2]/button[1]')))
    #remove_button.click()

    # 사이트 로딩 시간 고려 및 봇감지 방지
    time.sleep(2)
    
    #브랜드 이름 찾기
    find_brd = driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[2]/p[1]/button')
    driver.execute_script("arguments[0].scrollIntoView(true);", find_brd)
    brand_name=driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[2]/p[1]/button').text
    #all_brands.append(brand_name)

    brand_product_info_dic = {}
    #os.mkdir('glow_pick_img/{}'.format(brand_name))
    
    all_reviews = []
    all_users = []
    all_combined = []
    all_review_num = []
    all_rating = []
    all_date = []
    all_product_category_link = []
    all_product_ingredient_risk_num = []
    all_product_ingredient_risk = []
    all_award_names = []
    all_product_category= []
    all_award_sectors = []
    all_award_ranks = []
    all_product_store_name = []
    all_product_ingredient_rist_unknown =[]
    all_product_ingredient_rist_low =[]
    all_product_ingredient_rist_mid =[]
    all_product_ingredient_rist_high =[]
    all_product_ingredient_rist_unknown_kor = []
    all_product_ingredient_rist_unknown_eng = []
    all_product_ingredient_rist_unknown_desc = []
    all_product_ingredient_rist_low_kor = []
    all_product_ingredient_rist_low_eng = []
    all_product_ingredient_rist_low_desc = []
    all_product_ingredient_rist_mid_kor = []
    all_product_ingredient_rist_mid_eng = []
    all_product_ingredient_rist_mid_desc = []
    all_product_ingredient_rist_high_kor = []
    all_product_ingredient_rist_high_eng = []
    all_product_ingredient_rist_high_desc = []
    
    product_color_type =""
    time.sleep(3)
    html = driver.page_source
    soup = BeautifulSoup(html,"html.parser")
    award = ""
    award = soup.select('.info__article.award')
    
    if award != []:
        time.sleep(4)  
        
        element_product_rank = soup.find('span','icon product-rank')
        if element_product_rank != None:
            element = driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[2]')
            driver.execute_script("arguments[0].scrollIntoView(true);", element)
            
            award_click=driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[2]/h3/button')
            driver.execute_script("arguments[0].click();", award_click)# 수상내역 전체 보기 보기버튼
            time.sleep(3)
            html = driver.page_source
            soup = BeautifulSoup(html,"html.parser")
            award_names = soup.select('.awards__item__text__name')
            award_sectors = soup.select('.awards__item__text__award')
            award_ranks = soup.select('.awards__item__text__rank')
            time.sleep(5)
            driver.find_element_by_xpath('/html/body/div/div/div/div/div[1]/span/div/div[2]/h1/button').click()#수상내역 닫기
            time.sleep(2)
            element = driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[3]') 
            driver.execute_script("arguments[0].scrollIntoView(true);", element)
            time.sleep(3)
            
            #성분 구성 전체 보기
            driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[3]/h3/button').click()
            time.sleep(3)
            html = driver.page_source
            soup = BeautifulSoup(html,"html.parser")
            product_ingredient_risk_num = soup.select('.tag__bg__label')
            product_ingredient_risk = soup.select('.tag__legend')
            
            try:
                product_ingredient_list = soup.select('.ingredient__list__item.item')
                #print(len(product_ingredient_list))
                for index,li in enumerate(product_ingredient_list):

                    #print(li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--unknown"}))

                    if li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--unknown"}):
                        all_product_ingredient_rist_unknown.append(li)
                    if li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--low"}):
                        all_product_ingredient_rist_low.append(li)
                    if li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--mid"}):
                        all_product_ingredient_rist_mid.append(li)
                    if li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--high"}):
                        all_product_ingredient_rist_high.append(li)
            except:
                pass
            time.sleep(3)
            driver.find_element_by_class_name('details__contents__h1__button').click()#성분 구성 닫기
            time.sleep(3)

            element = driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[4]')
            driver.execute_script("arguments[0].scrollIntoView(true);", element)
            time.sleep(3)
            product_information = driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[4]/div/pre').text #제품설명
            driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[4]/h3/button').click()# 제품 설명 전체 보기 버튼클릭
            

            
            try:
                time.sleep(3)
                html = driver.page_source
                soup = BeautifulSoup(html,"html.parser")
                product_information = driver.find_element_by_class_name('descriptions__article__pre').text #제품설명
                
                if '컬러/타입' in str(soup.select('.descriptions__article')):
                    product_color_type=soup.select('.descriptions__article')[1].find('pre').get_text()
                else :
                    product_color_type=""

            except:
                product_color_type=""
                pass
                
            try:
                time.sleep(3)
                html = driver.page_source
                soup = BeautifulSoup(html,"html.parser")
                product_category =soup.select('.descriptions__article__category')#제품 카테고리(상위)
                product_category_link = soup.select('.descriptions__article__category.descriptions__article__category-link')#제품 카테고리(하위)
                product_stores_name = soup.select('.stores__store__name')
                
            except:
                pass
            
            
            
            time.sleep(2)
            driver.find_element_by_class_name('details__contents__h1__button').click()#제품 설명 닫기
            time.sleep(3)

            html = driver.page_source
            soup = BeautifulSoup(html,"html.parser")
            product_all_rank = driver.find_element_by_class_name('rank-item__rank').text #동일 제품군중 순위(글자)
            product_all_rank_name = driver.find_element_by_class_name('rank-item__name').text #동일 제품군중 순위(숫자)
            product_in_brand_rank = driver.find_element_by_class_name('rank-item__rank').text #브랜드내 순위(숫자)
            
            
            
        else:
            time.sleep(3)
            element = driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[1]') 
            driver.execute_script("arguments[0].scrollIntoView(true);", element)
            driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[1]/h3/button').click()# 수상내역 전체 보기 보기버튼
            time.sleep(3)
            html = driver.page_source
            soup = BeautifulSoup(html,"html.parser")
            award_names = soup.select('.awards__item__text__name')
            award_sectors = soup.select('.awards__item__text__award')
            award_ranks = soup.select('awards__item__text__rank')
            time.sleep(5)
            driver.find_element_by_xpath('/html/body/div/div/div/div/div[1]/span/div/div[2]/h1/button').click()#수상내역 닫기
            time.sleep(2)
            element = driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[2]') 
            driver.execute_script("arguments[0].scrollIntoView(true);", element)
            time.sleep(3)
            driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[2]/h3/button').click() #성분구성 전체 보기
            time.sleep(3)
            html = driver.page_source
            soup = BeautifulSoup(html,"html.parser")
            product_ingredient_risk_num = soup.select('.tag__bg__label')
            product_ingredient_risk = soup.select('.tag__legend')
            try:
                product_ingredient_list = soup.select('.ingredient__list__item.item')
                #print(len(product_ingredient_list))
                for index,li in enumerate(product_ingredient_list):

                    #print(li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--unknown"}))

                    if li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--unknown"}):
                        all_product_ingredient_rist_unknown.append(li)
                    if li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--low"}):
                        all_product_ingredient_rist_low.append(li)
                    if li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--mid"}):
                        all_product_ingredient_rist_mid.append(li)
                    if li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--high"}):
                        all_product_ingredient_rist_high.append(li)
            except:
                pass
            time.sleep(3)
            driver.find_element_by_class_name('details__contents__h1__button').click()#성분구성 닫기
            time.sleep(3)
            element = driver.find_element_by_class_name('info__article description')
            driver.execute_script("arguments[0].scrollIntoView(true);", element)
            time.sleep(3)
            product_information = driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[2]/div/pre').text #제품설명
            driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[4]/h3/button').click()# 제품 설명 전체보기 버튼클릭
           # driver.find_element_by_class_name('info__article__h3__button').click()
            time.sleep(3)
            product_information = driver.find_element_by_class_name('descriptions__article__pre').text # 제품설명 텍스트
            try:
                time.sleep(3)
                html = driver.page_source
                soup = BeautifulSoup(html,"html.parser")
                 #제품설명

                if '컬러/타입' in str(soup.select('.descriptions__article')):
                    product_color_type=soup.select('.descriptions__article')[1].find('pre').get_text()
                else :
                    product_color_type=""

            except:
                product_color_type=""
                pass
            
            product_category =soup.select('.descriptions__article__category')#제품 카테고리(상위)
            product_category_link = soup.select('.descriptions__article__category.descriptions__article__category-link')#제품 카테고리(하위)
            product_stores_name = soup.select('.stores__store__name')
            time.sleep(2)
           
        
            driver.find_element_by_class_name("descriptions__article")
            driver.find_element_by_class_name('details__contents__h1__button').click()#수상내역 닫기
            time.sleep(3)
            product_all_rank = []
            product_all_rank_name = []
            product_in_brand_rank = []
            
            
        for l in award_names:
            l = l.text
            l = l.replace(' ','')
            l = l.replace('\n','')
            all_award_names.append(l)
        for p in award_sectors:
            p = p.text
            p = p.replace(' ','')
            p = p.replace('\n','')
            all_award_sectors.append(p)
        for w in award_ranks:
            w = w.text
            w = w.replace(' ','')
            w = w.replace('\n','')
            all_award_ranks.append(w)
            
            
            
            
            
    elif award == []:
        html = driver.page_source
        soup = BeautifulSoup(html,"html.parser")
        element_product_rank = soup.find('span','icon product-rank')

        if element_product_rank != None:
            element = driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[2]')
            driver.execute_script("arguments[0].scrollIntoView(true);", element)
            driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[2]/h3/button').click()#성분 구성 전체 보기 보기버튼
            time.sleep(2)
            html = driver.page_source
            soup = BeautifulSoup(html,"html.parser")
            product_ingredient_risk_num = soup.select('.tag__bg__label')
            product_ingredient_risk = soup.select('.tag__legend')
            try:
                product_ingredient_list = soup.select('.ingredient__list__item.item')
                #print(len(product_ingredient_list))
                for index,li in enumerate(product_ingredient_list):

                    #print(li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--unknown"}))

                    if li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--unknown"}):
                        all_product_ingredient_rist_unknown.append(li)
                    if li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--low"}):
                        all_product_ingredient_rist_low.append(li)
                    if li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--mid"}):
                        all_product_ingredient_rist_mid.append(li)
                    if li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--high"}):
                        all_product_ingredient_rist_high.append(li)
            except:
                pass
            time.sleep(2)
            driver.find_element_by_xpath('/html/body/div/div/div/div/div[1]/span/div/div[2]/h1/button').click()

            time.sleep(3)
            element = driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]')
            driver.execute_script("arguments[0].scrollIntoView(true);", element)
            time.sleep(2)
            product_information = driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[3]/div/pre').text #제품설명
            driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[3]/h3/button').click()
            
            
            try:
                time.sleep(3)
                html = driver.page_source
                soup = BeautifulSoup(html,"html.parser")

                try:
                    product_color_type = driver.find_element_by_xpath('/html/body/div/div/div/div/div[1]/span/div/div[2]/div/article[2]/div').text

                except:
                    pass
                product_category = soup.select('.descriptions__article__category')
                product_category_link = soup.select('.descriptions__article__category.descriptions__article__category-link')
                product_stores_name = soup.select('.stores__store__name')
                #driver.find_element_by_class_name(‘details__contents__h1__button’).click()
            except:
                pass
            driver.find_element_by_xpath('/html/body/div/div/div/div/div[1]/span/div/div[2]/h1/button').click()
            time.sleep(3)

            html = driver.page_source
            soup = BeautifulSoup(html,"html.parser")
            product_all_rank = driver.find_element_by_class_name('rank-item__rank').text #동일 제품군중 순위(글자)
            product_all_rank_name = driver.find_element_by_class_name('rank-item__name').text #동일 제품군중 순위(숫자)
            product_in_brand_rank = driver.find_elements_by_class_name('rank-item__rank')#브랜드내 순위(숫자)
            product_in_brand_rank = product_in_brand_rank[1].text
        else:
            time.sleep(3)
            element = driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[1]')
            driver.execute_script("arguments[0].scrollIntoView(true);", element)
            driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[1]/h3/button').click()#성분 구성 전체 보기 보기버튼
            time.sleep(2)
            html = driver.page_source
            soup = BeautifulSoup(html,"html.parser")
            product_ingredient_risk_num = soup.select('.tag__bg__label')
            product_ingredient_risk = soup.select('.tag__legend')
            try:
                product_ingredient_list = soup.select('.ingredient__list__item.item')
                #print(len(product_ingredient_list))
                for index,li in enumerate(product_ingredient_list):

                    #print(li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--unknown"}))

                    if li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--unknown"}):
                        all_product_ingredient_rist_unknown.append(li)
                    if li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--low"}):
                        all_product_ingredient_rist_low.append(li)
                    if li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--mid"}):
                        all_product_ingredient_rist_mid.append(li)
                    if li.find("div",{"class" : "tag__bg tag__bg--item tag__bg--high"}):
                        all_product_ingredient_rist_high.append(li)
            except:
                pass

            time.sleep(2)
            driver.find_element_by_xpath('/html/body/div/div/div/div/div[1]/span/div/div[2]/h1/button').click()
            time.sleep(3)
            element = driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[2]')
            driver.execute_script("arguments[0].scrollIntoView(true);", element)
            time.sleep(2)
            product_information = driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[2]/div/pre').text
            driver.find_element_by_xpath('/html/body/div/div/div/div/main/div/section/div[3]/article[2]/h3/button').click()
          
        
            try:
                time.sleep(3)
                html = driver.page_source
                soup = BeautifulSoup(html,"html.parser")
                product_information = driver.find_element_by_class_name('descriptions__article__pre').text #제품설명
                
                if '컬러/타입' in str(soup.select('.descriptions__article')):
                    product_color_type=soup.select('.descriptions__article')[1].find('pre').get_text()
                else :
                    product_color_type=""

            except:
                product_color_type=""
                pass
                
            try:
                time.sleep(3)
                html = driver.page_source
                soup = BeautifulSoup(html,"html.parser")
                product_category =soup.select('.descriptions__article__category')#제품 카테고리(상위)
                product_category_link = soup.select('.descriptions__article__category.descriptions__article__category-link')#제품 카테고리(하위)
                product_stores_name = soup.select('.stores__store__name')
                
            except:
                pass
            

            driver.find_element_by_xpath('/html/body/div/div/div/div/div[1]/span/div/div[2]/h1/button').click()
            time.sleep(3)
            product_all_rank = []
            product_all_rank_name = []
            product_in_brand_rank = []

    for g in product_category:
        all_product_category.append(g.text)
    for x in product_ingredient_risk_num[:4]:
        x = x.text
        x = x.replace(' ','')
        x = x.replace('\n','')
        all_product_ingredient_risk_num.append(x)
    for j in product_ingredient_risk:
        j = j.text
        j = j.replace(' ','')
        j = j.replace('\n','')
        all_product_ingredient_risk.append(j)
    for k in product_category_link:
        k = k.text
        k = k.replace(' ','')
        k = k.replace('\n','')
        all_product_category_link.append(k)
    for m in product_stores_name:
            m = m.text
            m = m.replace(' ','')
            m = m.replace('\n','')
            all_product_store_name.append(m)
    for risk_unkown in all_product_ingredient_rist_unknown:
        if risk_unkown.find("p",{"class" : "item__wrapper__text__kor"}):
            risk_unkown_kor = risk_unkown.find("p",{"class" : "item__wrapper__text__kor"}).text
            risk_unkown_kor = risk_unkown_kor.replace('\n','')
            risk_unkown_kor = risk_unkown_kor.replace(' ','')
            all_product_ingredient_rist_unknown_kor.append(risk_unkown_kor)

        if risk_unkown.find("p",{"class" : "item__wrapper__text__eng"}):
            risk_unkown_eng = risk_unkown.find("p",{"class" : "item__wrapper__text__eng"}).text
            risk_unkown_eng = risk_unkown_eng.replace('\n','')
            risk_unkown_eng = risk_unkown_eng.replace(' ','')
            all_product_ingredient_rist_unknown_eng.append(risk_unkown_eng)

        if risk_unkown.find("p",{"class" : "item__wrapper__text__desc"}):
            risk_unkown_desc = risk_unkown.find("p",{"class" : "item__wrapper__text__desc"}).text
            risk_unkown_desc = risk_unkown_desc.replace('\n','')
            risk_unkown_desc = risk_unkown_desc.replace(' ','')
            all_product_ingredient_rist_unknown_desc.append(risk_unkown_desc)

    for risk_low in all_product_ingredient_rist_low:
        if risk_low.find("p",{"class" : "item__wrapper__text__kor"}):
            risk_low_kor = risk_low.find("p",{"class" : "item__wrapper__text__kor"}).text
            risk_low_kor = risk_low_kor.replace('\n','')
            risk_low_kor = risk_low_kor.replace(' ','')
            all_product_ingredient_rist_low_kor.append(risk_low_kor)

        if risk_low.find("p",{"class" : "item__wrapper__text__eng"}):
            risk_low_eng = risk_low.find("p",{"class" : "item__wrapper__text__eng"}).text
            risk_low_eng = risk_low_eng.replace('\n','')
            risk_low_eng = risk_low_eng.replace(' ','')
            all_product_ingredient_rist_low_eng.append(risk_low_eng)

        if risk_low.find("p",{"class" : "item__wrapper__text__desc"}):
            risk_low_desc = risk_low.find("p",{"class" : "item__wrapper__text__desc"}).text
            risk_low_desc = risk_low_desc.replace('\n','')
            risk_low_desc = risk_low_desc.replace(' ','')
            all_product_ingredient_rist_low_desc.append(risk_low_desc)

    for risk_mid in all_product_ingredient_rist_mid:
        if risk_mid.find("p",{"class" : "item__wrapper__text__kor"}):
            risk_mid_kor = risk_mid.find("p",{"class" : "item__wrapper__text__kor"}).text
            risk_mid_kor = risk_mid_kor.replace('\n','')
            risk_mid_kor = risk_mid_kor.replace(' ','')
            all_product_ingredient_rist_mid_kor.append(risk_mid_kor)

        if risk_mid.find("p",{"class" : "item__wrapper__text__eng"}):
            risk_mid_eng = risk_mid.find("p",{"class" : "item__wrapper__text__eng"}).text
            risk_mid_eng = risk_mid_eng.replace('\n','')
            risk_mid_eng = risk_mid_eng.replace(' ','')
            all_product_ingredient_rist_mid_eng.append(risk_mid_eng)

        if risk_mid.find("p",{"class" : "item__wrapper__text__desc"}):
            risk_mid_desc = risk_mid.find("p",{"class" : "item__wrapper__text__desc"}).text
            risk_mid_desc = risk_mid_desc.replace('\n','')
            risk_mid_desc = risk_mid_desc.replace(' ','')
            all_product_ingredient_rist_mid_desc.append(risk_mid_desc)

    for risk_high in all_product_ingredient_rist_high:
        if risk_high.find("p",{"class" : "item__wrapper__text__kor"}):
            risk_high_kor = risk_high.find("p",{"class" : "item__wrapper__text__kor"}).text
            risk_high_kor = risk_high_kor.replace('\n','')
            risk_high_kor = risk_high_kor.replace(' ','')
            all_product_ingredient_rist_high_kor.append(risk_high_kor)

        if risk_high.find("p",{"class" : "item__wrapper__text__eng"}):
            risk_high_eng = risk_high.find("p",{"class" : "item__wrapper__text__eng"}).text
            risk_high_eng = risk_high_eng.replace('\n','')
            risk_high_eng = risk_high_eng.replace(' ','')
            all_product_ingredient_rist_high_eng.append(risk_high_eng)

        if risk_high.find("p",{"class" : "item__wrapper__text__desc"}):
            risk_high_desc = risk_high.find("p",{"class" : "item__wrapper__text__desc"}).text
            risk_high_desc = risk_high_desc.replace('\n','')
            risk_high_desc = risk_high_desc.replace(' ','')
            all_product_ingredient_rist_high_desc.append(risk_high_desc)
    """
    try:
        comment = driver.find_elements_by_class_name('cutter__pre') #고객 리뷰
        user = driver.find_elements_by_class_name('info__details__nickname') #고객 아이디
        combined = driver.find_elements_by_class_name('property__wrapper__item') #고객 정보(나이, 피부타입, 성별)
        #review_num = driver.find_elements_by_class_name('property__reviews__count')#고객당리뷰횟수
        rating = soup.select('.stars__rating.font-spoqa')
        #rating = driver.find_elements_by_class_name('stars__rating font-spoqa')#제품 별점
        date = driver.find_elements_by_class_name('review__side-info__created-at')#리뷰작성날짜
        time.sleep(3)
        for d in date:
            all_date.append(d.text)
        for u in comment:
            u = u.text
            u = u.replace('\n','')
            all_reviews.append(u)
        for r in rating:
            r = r.text
            r = r.replace('\n','')
            r = r.replace(' ','')
            all_rating.append(r)
        for n in review_num:
            all_review_num.append(n.text)
        for c in combined:
            all_combined.append(c.text)
        for z in user:
            all_users.append(z.text)

    except:
        all_date.append('no date')
        all_reviews.append('no reviews')
        all_rating.append('no rating')
        all_review_num.append('no review')
        all_combined.append('no combiend')
        all_users.append('no user')

    """            
            
            
    time.sleep(3)
    product_url = driver.current_url #제품 url
    html = driver.page_source
    soup = BeautifulSoup(html,"html.parser")
    product_name = driver.find_element_by_class_name('product__summary__name').text #제품 이름
    amount_price = driver.find_element_by_class_name('offer__volume-price').text # 제품 용량/ 가격

    
    product_information = product_information.replace('\n','')
    product_id_index = product_url.rfind('/')
    product_id = product_url[product_id_index+1:]
    amount_price_index = amount_price.find('/')
    product_volume = amount_price[:amount_price_index-1]
    product_volume = product_volume.replace(' ','')
    product_price = amount_price[amount_price_index+1:]
    product_price = product_price.replace(' ','')
    
    brand_product_info_dic['brand_name'] = brand_name
    brand_product_info_dic['product_name'] = product_name #제품명3
    brand_product_info_dic['product_volume'] = product_volume #제품용량4
    brand_product_info_dic['product_price'] = product_price #제품가격5
    brand_product_info_dic['product_all_rank'] = product_all_rank # 유사 제품군 순위
    brand_product_info_dic['product_all_rank_name'] = product_all_rank_name # 유사 제품군 순위 타이틀
    brand_product_info_dic['product_brand_rank'] = product_in_brand_rank #브랜드 내 순위
    

    brand_product_info_dic['award_name'] = all_award_names #수상 타이틀
    brand_product_info_dic['award_sector'] = all_award_sectors# 수상 내역 부문
    brand_product_info_dic['award_rank'] = all_award_ranks # 수상 랭킹


    brand_product_info_dic['product_information'] = product_information #제품설명_텍스트
    brand_product_info_dic['product_color_type'] = product_color_type # 제품설명_컬러/타입
    brand_product_info_dic['all_product_store_name'] = all_product_store_name # 제품설명_판매처
    brand_product_info_dic['product_category'] = all_product_category # 제품설명_카테고리
    

    brand_product_info_dic['product_ingredient_risk'] = all_product_ingredient_risk # 위험도미정, 낮은위험도, 중간위험도, 높은위험도
    brand_product_info_dic['product_ingredient_risk_num'] = all_product_ingredient_risk_num # 성분 구성 개수
    
    brand_product_info_dic['product_ingredient_risk_unknown_kor'] = all_product_ingredient_rist_unknown_kor #위험도미정 성분 이름(한글)
    brand_product_info_dic['product_ingredient_risk_unknown_eng'] = all_product_ingredient_rist_unknown_eng #위험도미정 성분 이름(영어)
    brand_product_info_dic['product_ingredient_risk_unknown_desc'] = all_product_ingredient_rist_unknown_desc #위험도미정 성분 효과
    
    brand_product_info_dic['product_ingredient_risk_low_kor'] = all_product_ingredient_rist_low_kor#낮은위험도 성분 이름(한글)
    brand_product_info_dic['product_ingredient_risk_low_eng'] = all_product_ingredient_rist_low_eng#낮은위험도 성분 이름(영어)
    brand_product_info_dic['product_ingredient_risk_low_desc'] = all_product_ingredient_rist_low_desc#낮은위험도 성분 효과

    brand_product_info_dic['product_ingredient_risk_mid_kor'] = all_product_ingredient_rist_mid_kor#중간위험도 성분 이름(한글)
    brand_product_info_dic['product_ingredient_risk_mid_eng'] = all_product_ingredient_rist_mid_eng#중간위험도 성분 이름(영어)
    brand_product_info_dic['product_ingredient_risk_mid_desc'] = all_product_ingredient_rist_mid_desc#중간위험도 성분 효과

    brand_product_info_dic['product_ingredient_risk_high_kor'] = all_product_ingredient_rist_high_kor#높은위험도 성분 이름(한글)
    brand_product_info_dic['product_ingredient_risk_high_eng'] = all_product_ingredient_rist_high_eng#높은위험도 성분 이름(영어)
    brand_product_info_dic['product_ingredient_risk_high_desc'] = all_product_ingredient_rist_high_desc#높은위험도 성분 효과

    

    #brand_product_info_dic['reviews'] = all_reviews
    #brand_product_info_dic['date']= all_date
    #brand_product_info_dic['rating']= all_rating
    #brand_product_info_dic['review_num'] = all_review_num
    #brand_product_info_dic['combined'] = all_combined
    #brand_product_info_dic['user'] = all_users


    
    return brand_product_info_dic