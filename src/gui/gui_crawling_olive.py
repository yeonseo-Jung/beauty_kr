import os
import sys
import pickle
import pandas as pd
from datetime import datetime

from PyQt5 import uic
from PyQt5.QtWidgets import QMainWindow, QMessageBox, QFileDialog

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
form_path = os.path.join(form_dir, 'crawlingOliveInfoRevWindow.ui')
form = uic.loadUiType(form_path)[0]

from access_database.access_db import AccessDataBase
from multithreading.thread_crawling_oliveyoung import ThreadCrawlingOlive
from multithreading.thread_crawling_oliveyoung import ThreadCrawlingOliveUrl
from gui.table_view import TableViewer

class CrawlingOliveWindow(QMainWindow, form):
    ''' Product Info Crawling Window '''
    
    def __init__(self):
        super().__init__()    
        self.setupUi(self)
        self.setWindowTitle('Update Oliveyoung Products')
        self.viewer = None
        
        # class 
        self.crw = ThreadCrawlingOlive()
        self.crw_url = ThreadCrawlingOliveUrl()
        
        # path
        self.category_ids_path = os.path.join(tbl_cache, 'category_ids.txt')
        self.info_df_path = os.path.join(tbl_cache, 'info_df.csv')
        self.urls_path = os.path.join(tbl_cache, 'urls.txt')
        self.info_detail_df_path = os.path.join(tbl_cache, 'info_detail_df.csv')
        self.review_df_path = os.path.join(tbl_cache, 'review_df.csv')
        self.prg_path = os.path.join(tbl_cache, 'prg_dict.txt')
        
        # connect thread
        self.crw.progress.connect(self.update_progress)
        self.crw_url.progress.connect(self._update_progress)
        
        # connect func & btn
        self.Select.clicked.connect(self._select)
        self.Run_2.clicked.connect(self._run_url)
        self.Run.clicked.connect(self._run)
        self.Stop.clicked.connect(self.crw_url.stop)
        self.Pause.clicked.connect(self.crw.stop)
        self.View.clicked.connect(self.tbl_viewer)
        self.Save_2.clicked.connect(self._save_file)
        self.Save.clicked.connect(self.save_file)
        # self.Upload.clicked.connect(self._upload)
        
        # category toggled
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
        
    def categ_toggled(self):
        categs = []
        
        if self.skincare.isChecked():
            categ = "스킨케어"
            categs.append(categ)
            
        if self.bodycare.isChecked():
            categ = "바디케어"
            categs.append(categ)
            
        if self.makeup.isChecked():
            categ = "메이크업"
            categs.append(categ)
            
        if self.haircare.isChecked():
            categ = "헤어케어"
            categs.append(categ)
            
        if self.cleansing.isChecked():
            categ = "클렌징"
            categs.append(categ)
            
        if self.menscare.isChecked():
            categ = "맨즈케어"
            categs.append(categ)
            
        if self.suncare.isChecked():
            categ = "선케어"
            categs.append(categ)
            
        if self.maskpack.isChecked():
            categ = "마스크/팩"
            categs.append(categ)
            
        if self.beauty_tool.isChecked():
            categ = "뷰티툴"
            categs.append(categ)
            
        if self.fragrance.isChecked():
            categ = "프래그런스"
            categs.append(categ)
            
        return categs
    
    def _select(self):
        
        category_dict = {
            '스킨케어': ['00010008', '00010009', '00010010', '00080008', '00080009', '00080010'],
            '메이크업': ['00020006', '00020001', '00020007'],
            '바디케어': ['00080004', '00030005', '00030014', '00030013', '00030016', '00030008', '00030015', '00030012', '00030018', '00030017'],
            '헤어케어': ['00040008', '00040007', '00040009', '00040010', '00040011', '00040012', '00040013'],
            '클렌징': ['00100001', '00100002', '00100003', '00080006'],
            '선케어': ['00110001', '00110002', '00080005'],
            '맨즈케어': ['00070007', '00070008', '00070009', '00070010', '00070011', '00070012', '00070015'],
            '마스크/팩': ['00090001', '00090002', '00090003', '00080011'],
            '프래그런스': ['00050002', '00050003', '00050004'],
            '뷰티툴': ['00060001', '00060002', '00060003', '00060004', '00060005', '00060006', '00060007']
        }

        categs = self.categ_toggled()
        if len(categs) == 0:
            msg = QMessageBox()
            msg.setText('** 한 개 이상의 카테고리를 선택하세요 **')
            msg.exec_()
        else:
            category_ids = []
            for categ in categs:
                category_ids += category_dict[categ]
                
            msg = QMessageBox()
            msg.setText('Selection done!')
            msg.exec_()
            
            with open(self.category_ids_path, 'wb') as f:
                pickle.dump(category_ids, f)
            
    def update_progress(self, progress):
        
        if os.path.isfile(self.prg_path):
            with open(self.prg_path, 'rb') as f:
                prg_dict_ = pickle.load(f)
            itm_ = prg_dict_['n'] 
            elapsed_ = round(prg_dict_['elapsed'], 0)
            
        else:
            itm_, elapsed_ = 0, 0
        
        prg_dict = progress.format_dict
        itm = prg_dict['n'] + itm_
        tot = prg_dict['total'] + itm_ 
        per = int(round((itm / tot) * 100, 0))
        elapsed = round(prg_dict['elapsed'], 0) + elapsed_
        prg_dict_ = {
            'n': itm,
            'elapsed': elapsed,
        }
                
        if itm >= 1:
            remain_time = round((elapsed * tot / itm) - elapsed, 0)
        else:
            remain_time = 0
        
        self.progressBar.setValue(per)
        
        elapsed_h = int(elapsed // 3600)
        elapsed_m = int((elapsed % 3600) // 60)
        elapsed_s = int(elapsed - (elapsed_h * 3600 + elapsed_m * 60))
        
        remain_h = int(remain_time // 3600)
        remain_m = int((remain_time % 3600) // 60)
        remain_s = int(remain_time - (remain_h * 3600 + remain_m * 60))
        
        message = f"{per}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s}"
        self.statusbar.showMessage(message)
        
        # pause 시에 현재까지 진행률 저장
        if not self.crw.power:
            with open(self.prg_path, 'wb') as f:
                pickle.dump(prg_dict_, f)
            
            if itm == tot:
                message = f"{per}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s} **Complete**"
                os.remove(self.prg_path)
            else:
                message = f"{per}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s} **PAUSE**"
            self.statusbar.showMessage(message)
                
    def _update_progress(self, progress):
        
        prg_dict = progress.format_dict
        itm = prg_dict['n'] 
        tot = prg_dict['total']
        per = int(round((itm / tot) * 100, 0))
        elapsed = int(round(prg_dict['elapsed'], 0))
        if itm >= 1:
            remain_time = int(round((elapsed * tot / itm) - elapsed, 0))
        else:
            remain_time = 0
        
        self.progressBar_2.setValue(per)
        
        message = f"{per}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed}s < Remain time: {remain_time}s "
        self.statusbar.showMessage(message)
        
        if not self.crw_url.power:
            with open(self.urls_path, 'rb') as f:
                urls = pickle.load(f)
            products = len(urls)
            time = round(products * 250 / 3600, 2)
            self.Products.display(products)
            self.Time.display(time)
            
    def _run_url(self):
        msg = QMessageBox()
        if os.path.exists(self.category_ids_path):
            if self.crw_url.power:
                msg.setText('** 크롤링 진행 중입니다 **')
                msg.exec_()
            else:
                msg.setText("- 인터넷 연결 확인 \n- VPN 연결 확인 \n- mac 자동 잠금 해제 확인")
                msg.exec_()
                self.crw_url.power = True
                self.crw_url.start()
        else:
            msg.setText('** 카테고리 선택 후 진행하세요 **')
            msg.exec_()
        
    def _run(self):
        msg = QMessageBox()
        if os.path.exists(self.urls_path):
            if self.crw.power:
                msg.setText('** 스크레이핑 진행 중입니다 **')
                msg.exec_()
            else:
                msg.setText("- 인터넷 연결 확인 \n- VPN 연결 확인 \n- mac 자동 잠금 해제 확인")
                msg.exec_()
                self.crw.power = True
                self.crw.start()
        else:
            msg.setText('** url 수집 후 진행하세요 **')
            msg.exec_()
            
    def _save_file(self):
        ''' save csv file '''
        
        # 캐시에 해당 파일이 존재할 때 저장
        if os.path.isfile(self.info_df_path):
            df = pd.read_csv(self.info_df_path)
            file_save = QFileDialog.getSaveFileName(self, "Save File", "", "csv file (*.csv)")
            
            if file_save[0] != "":
                df.to_csv(file_save[0], index=False)
        else:
            msg = QMessageBox()
            msg.setText('** 일시정지 후 다시 시도해주세요 **')
            msg.exec_()
            
    def save_file(self):
        ''' save csv file '''
        
        # 캐시에 해당 파일이 존재할 때 저장
        if os.path.isfile(self.info_detail_df_path):
            df = pd.read_csv(self.info_detail_df_path)
            file_save = QFileDialog.getSaveFileName(self, "Save File", "", "csv file (*.csv)")
            
            if file_save[0] != "":
                df.to_csv(file_save[0], index=False)
        else:
            msg = QMessageBox()
            msg.setText('** 일시정지 후 다시 시도해주세요 **')
            msg.exec_()
            
    def tbl_viewer(self):
        ''' table viewer '''
        
        # 캐시에 테이블이 존재할 때 open table viewer
        if os.path.isfile(self.info_detail_df_path):
            if self.viewer is None:
                self.viewer = TableViewer()
            else:
                self.viewer.close()
                self.viewer = TableViewer()
                
            self.viewer.show()
            self.viewer._loadFile('info_detail_df.csv')
        else:
            msg = QMessageBox()
            msg.setText('** 일시정지 후 다시 시도해주세요 **')
            msg.exec_()