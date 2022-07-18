import os
import re
import sys
import time
import pickle
import pandas as pd

from PyQt5 import uic
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QMainWindow, QMessageBox, QFileDialog, QListWidgetItem

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
    form_dir = os.path.join(root, 'form')
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src = os.path.abspath(os.path.join(cur_dir, os.pardir))
    sys.path.append(src)
    form_dir = os.path.join(src, 'gui/form')
    
tbl_cache = os.path.join(root, 'tbl_cache')
conn_path = os.path.join(root, 'conn.txt')
form_path = os.path.join(form_dir, 'reviewWindow.ui')
form = uic.loadUiType(form_path)[0]

from access_database.access_db import AccessDataBase
from reviews._preprocess import ReviewMapping
from gui.table_view import TableViewer

class ReviewWindow(QMainWindow, form):
    ''' Product Info Crawling Window '''
    
    def __init__(self):
        super().__init__()    
        self.setupUi(self)
        self.setWindowTitle('Mapping Review and Upload')
        
        # check     
        self.select_ck = False
        self.dup_ck = False
        self.view_ck = None
        
        # path
        self.name = 'reviews_upload.csv'
        self.path = os.path.join(tbl_cache, self.name)
        
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = AccessDataBase(conn[0], conn[1], conn[2])
        
        self.review = ReviewMapping()
        
        # get table
        for table in self._get_tbl():
            item = QListWidgetItem(table)
            # item.setCheckState(Qt.Unchecked)
            self.TableList.addItem(item)
        
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
        self.menscare.setChecked(False)
        self.menscare.toggled.connect(self.categ_toggled)
        self.suncare.setChecked(False)
        self.suncare.toggled.connect(self.categ_toggled)
        self.maskpack.setChecked(False)
        self.maskpack.toggled.connect(self.categ_toggled)
        self.beauty_tool.setChecked(False)
        self.beauty_tool.toggled.connect(self.categ_toggled)
        self.fragrance.setChecked(False)
        self.fragrance.toggled.connect(self.categ_toggled)
        
        # connect button
        self.Select.clicked.connect(self._select)
        self.Dup_check.clicked.connect(self._dup_check)
        self.View.clicked.connect(self._view)
        self.Save.clicked.connect(self._save)
        # self.Status.clicked.connect(self._status)
        # self.Upload.clicked.connect(self._upload)
        
    def _get_tbl(self):
        ''' db에서 매핑 대상 테이블만 가져오기 '''
        
        tables = self.db.get_tbl_name()
        reg = re.compile('naver_beauty_product_info_extended_v[0-9]+_review')
        table_list = []
        for tbl in tables:
            tbl_ = re.match(reg, tbl)
            if tbl_:
                table_list.append(tbl_.group(0))
        table_list = sorted(list(set(table_list)))
        table_list.append('oliveyoung_product_info_final_version_review')
        return table_list
        
    def categ_toggled(self):
        categs, categs_en = [], []
        
        if self.skincare.isChecked():
            categs.append("스킨케어")
            categs_en.append("skin_care")
            
        if self.bodycare.isChecked():
            categs.append("바디케어")
            categs_en.append("body_care")
            
        if self.makeup.isChecked():
            categs.append("메이크업")
            categs_en.append("makeup")
            
        if self.haircare.isChecked():
            categs.append("헤어케어")
            categs_en.append("hair_care")
            
        if self.cleansing.isChecked():
            categs.append("클렌징")
            categs_en.append("cleansing")
            
        if self.menscare.isChecked():
            categs.append("맨즈케어")
            categs_en.append("mens_care")
            
        if self.suncare.isChecked():
            categs.append("선케어")
            categs_en.append("sun_care")
            
        if self.maskpack.isChecked():
            categs.append("마스크/팩")
            categs_en.append("mask_pack")
            
        if self.beauty_tool.isChecked():
            categs.append("뷰티툴")
            categs_en.append("beauty_tool")
            
        if self.fragrance.isChecked():
            categs.append("프래그런스")
            categs_en.append("fragrance")
            
        return categs, categs_en
        
    def _select(self):
        ''' Select review table '''
        
        categs, categs_en = self.categ_toggled()
        if len(categs_en) == 1:
            msg = QMessageBox()
            msg.setText(f'** 대용량 데이터 Select **/n 5분이상 소요예정 입니다')
            msg.exec_()
            
            self.category = categs_en[0]
            self.review.select(self.category)
            
            msg = QMessageBox()
            msg.setText('** Select table successful! **')
            msg.exec_()
            
            self.select_ck = True
        else:
            msg = QMessageBox()
            msg.setText('** 한개의 카테고리를 선택해주세요 **')
            msg.exec_()
            
    def _dup_check(self):
        ''' Duplicate check & Create table '''
        
        msg = QMessageBox()
        if self.select_ck:
            self.review.dup_check()
            self.review.create()
            self.df = pd.read_csv(self.path, lineterminator='\n')
            
            msg.setText('** Duplicate check successful! **')
            msg.exec_()
            
            self.select_ck = False
            self.dup_ck = True
        else:
            msg.setText('** Select 완료 후 시도하세요 **')
            msg.exec_()
        
    def _viewer(self, msg_txt):
        ''' table viewer '''
            
        # 캐시에 테이블이 존재할 때 open table viewer 
        if os.path.isfile(self.path):
            if self.view_ck is None:
                self.view_ck = TableViewer()
            else:
                self.view_ck.close()
                self.view_ck = TableViewer()
                
            self.view_ck.show()
            self.view_ck._loadFile(self.name)
            
        else:
            msg = QMessageBox()
            msg.setText(msg_txt)
            msg.exec_()
            
    def _saver(self, msg_txt):
        ''' save csv file '''
        
        # 캐시에 해당 파일이 존재할 때 저장
        if os.path.isfile(self.path):
            file_save = QFileDialog.getSaveFileName(self, "Save File", f'beauty_kr_{self.category}_reviews_all', "csv file (*.csv)")
            
            if file_save[0] != "":
                self.df.to_csv(file_save[0], index=False)
        else:
            msg = QMessageBox()
            msg.setText(msg_txt)
            msg.exec_()
        
    def _view(self):
        msg = "** Duplicate Check 완료 후 시도하세요 **"
        self._viewer(msg)
            
    def _save(self):
        msg = "** Duplicate Check 완료 후 시도하세요 **"
        self._saver(msg)
        
    def _status(self):
        msg = QMessageBox()
        if self.df is None:
            msg.setText("** Duplicate Check 완료 후 시도하세요 **")
            msg.exec_()
        else:
            unique_len = len(self.df.item_key.unique())
            reviews_len = len(self.df)
            msg_txt = f"상품 수: {unique_len}\n리뷰 수: {reviews_len}"
            msg.setText(msg_txt)
            msg.exec_()
        
    # def _upload(self):
    #     msg = QMessageBox()
    #     msg_txt = f"<Create Table>\n테이블 명: {self.table_name}\n** 대용량 리뷰 데이터 업로드: 10분 이상 소요됩니다 **"
    #     msg.setText(msg_txt)
    #     msg.exec_()
    #     self.db.engine_upload(upload_df=self.upload_df, table_name=self.table_name, if_exists_option='replace', pk='pk')
    #     self.db.engine_upload(upload_df=self.info_df_all, table_name=f'glowpick_product_info_{self.category}', if_exists_option='replace', pk='id')
    #     msg = QMessageBox()
    #     msg_txt = f"<테이블 업로드 완료>\n\n리뷰 테이블 명: {self.table_name}\n개체 테이블 명: glowpick_product_info_{self.category}"
    #     msg.setText(msg_txt)
    #     msg.exec_()