import os
import re
import sys
import time
# import pickle
import numpy as np
import pandas as pd

# Scrapping
from bs4 import BeautifulSoup
from selenium import webdriver
from user_agent import generate_user_agent
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.common.alert import Alert
from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.common.exceptions import UnexpectedAlertPresentException, NoAlertPresentException, NoSuchElementException, TimeoutException

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
    
try:
    from hangle import _distance
except:
    pass

'''TLS CA error solution'''
def override_where():
    """ overrides certifi.core.where to return actual location of cacert.pem """
    # change this to match the location of cacert.pem
    return os.path.join(root, '_certifi', 'cacert.pem')

def replace_certifi():
    if hasattr(sys, "frozen"):
        import certifi.core

        os.environ["REQUESTS_CA_BUNDLE"] = override_where()
        certifi.core.where = override_where

        # delay importing until after where() has been replaced
        import requests.utils
        import requests.adapters
        # replace these variables in case these modules were
        # imported before we replaced certifi.core.where
        requests.utils.DEFAULT_CA_BUNDLE_PATH = override_where()
        requests.adapters.DEFAULT_CA_BUNDLE_PATH = override_where()

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
            
            # # tls ca error solution
            # if 'TLS CA' in str(e):
            #     replace_certifi()
            
            time.sleep(300)
            try:
                wd.quit()
            except:
                pass
            wd = None
    return wd
    
def scroll_down(wd, sleep_time, check_count):
    ''' page scroll down '''
    
    cnt = 0
    while True:
        # height = wd.execute_script("return document.body.scrollHeight")
        wd.find_element(By.TAG_NAME, 'body').send_keys(Keys.END)
        time.sleep(sleep_time)
        cnt += 1
        
        if cnt == check_count:
            break        
    return wd