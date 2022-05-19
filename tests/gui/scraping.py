import os
import re
import sys
import time
import pickle
import pandas as pd
from tqdm.auto import tqdm

cur_dir = os.path.dirname(os.path.realpath(__file__))
root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
src = os.path.abspath(os.path.join(cur_dir, os.pardir))
tbl_cache = root + '/tbl_cache'
sys.path.append(root)
sys.path.append(src)
sys.path.append(tbl_cache)

from access_database import access_db
from scraping import scraper_naver


import sys
from PyQt5 import uic
from PyQt5 import QtCore
from PyQt5.QtWidgets import *

scraping_form = uic.loadUiType(cur_dir + '/form/scrapingWindow.ui')[0]

class CrawlingWindow(QMainWindow, scraping_form):
    ''' Product Info Crawling Window '''
    
    def __init__(self):
        super().__init__()    
        self.setupUi(self)
        self.setWindowTitle('Crawling Product Info')
        
        
        # db 연결
        with open(cur_dir + '/conn.txt', 'rb') as f:
            conn = pickle.load(f)
        self.db = access_db.AccessDataBase(conn[0], conn[1], conn[2])
        
        self.skincare.setChecked(False)
        self.skincare.toggled.connect(self.categ_toggled)
        self.bodycare.setChecked(False)
        self.bodycare.toggled.connect(self.categ_toggled)
        self.makeup.setChecked(False)
        self.makeup.toggled.connect(self.categ_toggled)
        self.haircare.setChecked(False)
        self.haircare.toggled.connect(self.categ_toggled)
        self.cleansing.setChecked(False)
        self.cleansing.toggled.connect(self.categ_toggled)
        self.manscare.setChecked(False)
        self.manscare.toggled.connect(self.categ_toggled)
        self.suncare.setChecked(False)
        self.suncare.toggled.connect(self.categ_toggled)
        self.maskpack.setChecked(False)
        self.maskpack.toggled.connect(self.categ_toggled)
        
        self.accept.clicked.connect(self.get_tbl)
        self.start.clicked.connect(self.scraping)
        
    def update_progress(self, progress):
        
        prg_dict = progress.format_dict
        itm = prg_dict['n'] 
        tot = prg_dict['total']
        per = round((itm / tot) * 100, 0)
        elapsed = round(prg_dict['elapsed'], 0)
        if itm >= 1:
            remain_time = round((elapsed * tot / itm) - elapsed, 0)
        else:
            remain_time = 0
        
        self.progressBar.setValue(per)
        
        message = f"{int(per)}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed}s < Remain time: {remain_time}s "
        self.statusbar.showMessage(message)
        
    
    
    def categ_toggled(self):
        categs = []
        
        if self.skincare.isChecked():
            categ = "스킨케어"
            categs.append(categ)
            
        if self.bodycare.isChecked():
            categ = "배쓰&바디"
            categs.append(categ)
            
        if self.makeup.isChecked():
            categ_ = ["페이스메이크업", "아이메이크업", "립메이크업"]
            for categ in categ_:
                categs.append(categ)
            
        if self.haircare.isChecked():
            categ = "헤어"
            categs.append(categ)
            
        if self.cleansing.isChecked():
            categ = "클렌징"
            categs.append(categ)
            
        if self.manscare.isChecked():
            categ = "남성화장품"
            categs.append(categ)
            
        if self.suncare.isChecked():
            categ = "선케어"
            categs.append(categ)
            
        if self.maskpack.isChecked():
            categ = "마스크/팩"
            categs.append(categ)
            
        return categs
        
            
    def get_tbl(self):
        
        # table load
        table_name = 'glowpick_product_info_final_version'
        columns = ['id', 'brand_name', 'product_name', 'selection', 'division', 'groups']
        df = self.db.get_tbl(table_name, columns)
        map_tbl = self.db.get_tbl('naver_glowpick_mapping_table', 'all')
        
        
        map_id_uniq = map_tbl.loc[:, ['glowpick_product_info_final_version_id']].rename(columns={'glowpick_product_info_final_version_id': 'id'}).drop_duplicates(subset=['id'], keep='first').reset_index(drop=True)
        prd_scrap = pd.concat([df, map_id_uniq]).drop_duplicates(subset=['id'], keep=False).reset_index(drop=True)
        
        
        categs = self.categ_toggled()
        
        if categs == 0:
            msg = QMessageBox()
            msg.setText('Please select one or more categories.')
            msg.exec_()
            
        else:
            # 선택된 카테고리에 해당하는 상품만 크롤링 대상 테이블에 할당 
            index_list = []
            for categ in categs:
                index_list += prd_scrap.loc[prd_scrap.selection==categ].index.tolist()
                
            prds = prd_scrap.loc[index_list].reset_index(drop=True)
            
            # 브랜드 수, 상품 수 표시 
            brd_cnt = len(prds.brand_name.unique())
            prd_cnt = len(prds)
            self.brand_count.display(brd_cnt)
            self.product_count.display(prd_cnt)
            
            prds.to_csv(tbl_cache + '/prds_scrap.csv', index=False)
        
    def scraping(self):
        self.scrap = scraper_naver.ThreadCrawling()
        self.scrap.progress.connect(self.update_progress)
        self.scrap.start()