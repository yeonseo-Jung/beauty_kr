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
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src = os.path.abspath(os.path.join(cur_dir, os.pardir))
    sys.path.append(root)
    sys.path.append(src)
    base_path = os.path.dirname(os.path.realpath(__file__))
    tbl_cache = os.path.join(root, 'tbl_cache')
    
conn_path = os.path.join(base_path, 'conn.txt')
form_path = os.path.join(base_path, 'form/reviewWindow.ui')

from access_database import access_db
from reviews import _preprocess
from gui.table_view import TableViewer

review_form = uic.loadUiType(form_path)[0]

class ReviewWindow(QMainWindow, review_form):
    ''' Product Info Crawling Window '''
    
    def __init__(self):
        super().__init__()    
        self.setupUi(self)
        self.setWindowTitle('Mapping Review and Upload')
        self.viewer = None
        self.rev_info = None
        self.file_path = None
        
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = access_db.AccessDataBase(conn[0], conn[1], conn[2])
        
        self.review = _preprocess.ReviewMapping()
        
        # get table
        for table in self._get_tbl():
            item = QListWidgetItem(table)
            item.setCheckState(Qt.Unchecked)
            self.TableList.addItem(item)
            
        self.Import.clicked.connect(self._import_tbl)
        self.view_table_0.clicked.connect(self._viewer_0)
        self.save_0.clicked.connect(self._save_0)
        
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
        
        # dedup # sorting
        self.Dup_check.clicked.connect(self._dup_check)
        # status
        self.Status.clicked.connect(self._status)
        # table upload to db
        self.Upload.clicked.connect(self._upload)
        
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
        return table_list
    
    def _import_tbl(self):
        ''' 데이터 베이스에서 테이블 가져와서 통합하기 '''
        
        # 매핑 대상 테이블
        tbls = []
        for idx in range(self.TableList.count()):
            if self.TableList.item(idx).checkState() == Qt.Checked:
                tbls.append(self.TableList.item(idx).text())
        
        if len(tbls) == 0:
            msg = QMessageBox()
            msg.setText(f'Please check the table')
            msg.exec_()
            
        else:
            msg = QMessageBox()
            msg.setText(f'** 대용량 리뷰 데이터 임포트: 10분 이상 소요 예상됩니다 **')
            msg.exec_()
            
            st = time.time()
            # get table from db
            map_tbl, info_0, review_0, review_1 = self.review.get_table(tbls)
            ed = time.time()
            print(f'\n\nImport Time: {round(ed-st, 1)}sec\n\n')
            # mapping review
            review_0_mapped, review_1_mapped = self.review._mapping(map_tbl, review_0, review_1)
            # integration table
            self.rev_info = self.review._integ(info_0, review_0_mapped, review_1_mapped)
            
            msg = QMessageBox()
            msg.setText(f'Table import success')
            msg.exec_()
        
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
        
    def _dup_check(self):
        categs, categs_en = self.categ_toggled()
        
        if len(categs) == 0:
            msg = QMessageBox()
            msg.setText('한개 이상의 카테고리를 선택해주세요')
            msg.exec_()
        
        if str(type(self.rev_info)) == "<class 'pandas.core.frame.DataFrame'>":
            # integration selected category 
            index_list = []
            for categ in categs:    
                index_list += self.rev_info.loc[self.rev_info.category==categ].index.tolist()
            self.rev_info_categ = self.rev_info.loc[index_list].reset_index(drop=True)
            
            # dup check & sorting 
            self.dedup = self.review.dup_check(self.rev_info_categ)
            self.upload_df = self.review.upload_review_table(self.dedup) 
            
            # save table to cache dir
            category = "_".join(categs_en)
            self.table_name = f"beauty_kr_{category}_reviews_all"
            self.file_name = f"{self.table_name}.csv"
            self.file_path = os.path.join(tbl_cache, self.file_name)
            self.upload_df.to_csv(self.file_path, index=False)
            
            msg = QMessageBox()
            msg.setText(f'Completion Deduplication\n\ncategory: {category}')
            msg.exec_()
        else:
            msg = QMessageBox()
            msg.setText(f'테이블 임포트 완료 후 시도하세요')
            msg.exec_()
        
    def tbl_viewer(self, msg_txt):
        ''' table viewer '''
        
        if str(type(self.file_path)) == "<class 'NoneType'>":
            msg = QMessageBox()
            msg.setText(msg_txt)
            msg.exec_()
            
        # 캐시에 테이블이 존재할 때 open table viewer 
        elif os.path.isfile(self.file_path):
            if self.viewer is None:
                self.viewer = TableViewer()
            else:
                self.viewer.close()
                self.viewer = TableViewer()
                
            self.viewer.show()
            self.viewer._loadFile(self.file_name)
            
        else:
            msg = QMessageBox()
            msg.setText(msg_txt)
            msg.exec_()
            
    def save_file(self, msg_txt):
        ''' save csv file '''
        
        if str(type(self.file_path)) == "<class 'NoneType'>":
            msg = QMessageBox()
            msg.setText(msg_txt)
            msg.exec_()
        
        # 캐시에 해당 파일이 존재할 때 저장
        elif os.path.isfile(self.file_path):
            df = pd.read_csv(self.file_path, lineterminator='\n')
            file_save = QFileDialog.getSaveFileName(self, "Save File", self.table_name, "csv file (*.csv)")
            
            if file_save[0] != "":
                df.to_csv(file_save[0], index=False)
        else:
            msg = QMessageBox()
            msg.setText(msg_txt)
            msg.exec_()
        
    def _viewer_0(self):
        msg = "Duplicate Check 완료 후 시도하세요"
        self.tbl_viewer(msg)
            
    def _save_0(self):
        msg = "Duplicate Check 완료 후 시도하세요"
        self.save_file(msg)
        
    def _status(self):
        unique_len = len(self.upload_df.item_key.unique())
        reviews_len = len(self.upload_df)
        msg = QMessageBox()
        msg_txt = f"상품 수: {unique_len}\n리뷰 수: {reviews_len}"
        msg.setText(msg_txt)
        msg.exec_()
        
    def _upload(self):
        msg = QMessageBox()
        msg_txt = f"<Create Table>\n테이블 명: {self.table_name}\n** 대용량 리뷰 데이터 업로드: 10분 이상 소요됩니다 **"
        msg.setText(msg_txt)
        msg.exec_()
        self.db.engine_upload(upload_df=self.upload_df, table_name=self.table_name, if_exists_option='replace', pk='pk')