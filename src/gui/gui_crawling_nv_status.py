import os
import re
import sys
import pickle
import pandas as pd
from tqdm.auto import tqdm

from PyQt5 import uic
from PyQt5.QtCore import Qt
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
form_path = os.path.join(form_dir, 'crawlingNvStatus.ui')
form = uic.loadUiType(form_path)[0]

from multithreading.thread_crawling_naver import ThreadCrawlingNvStatus
from gui.table_view import TableViewer

class CrawlingNvStatus(QMainWindow, form):
    ''' Product Status, Store Crawling Window '''
    
    def __init__(self):
        super().__init__()    
        self.setupUi(self)
        self.setWindowTitle('Update Naver Products Sales Status')
        self.viewer = None
        
        # connect thread class 
        self.thread_crw = ThreadCrawlingNvStatus()
        self.thread_crw.progress.connect(self.update_progress)
        
        # cache file path
        self.path_input_df = os.path.join(tbl_cache, 'input_df.csv')
        self.path_scrape_df = os.path.join(tbl_cache, 'scrape_df.csv')
        self.category_list = os.path.join(tbl_cache, 'category_list.txt')
        self.path_prg = os.path.join(tbl_cache, 'prg_dict.txt')
        
        # connect func & btn
        self.Select.clicked.connect(self._select)
        self.Run.clicked.connect(self._run)
        self.Pause.clicked.connect(self.thread_crw.stop)
        self.View.clicked.connect(self.tbl_viewer)
        self.Save.clicked.connect(self.save_file)
        self.Upload.clicked.connect(self._upload_df)
        
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
        
    def update_progress(self, progress):

        if os.path.isfile(self.path_prg):
            with open(self.path_prg, 'rb') as f:
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
        if not self.thread_crw.power:
            with open(self.path_prg, 'wb') as f:
                pickle.dump(prg_dict_, f)
                
            if itm == tot:
                message = f"{per}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s} **Complete**"
                os.remove(self.path_prg)
            else:
                message = f"{per}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s} **PAUSE**"
            self.statusbar.showMessage(message)
        
        # ip 차단 및 db 연결 끊김 대응
        if self.thread_crw.check == 1:
            msg = QMessageBox()
            msg.setText("\n    ** ip 차단됨 **\n\n - VPN 나라변경 필요\n - wifi 재연결 필요")
            msg.exec_()
            
        elif self.thread_crw.check == 2:
            msg = QMessageBox()
            msg.setText("\n    ** db 연결 끊김 **\n\n - VPN, wifi 재연결 필요\n\n - Upload 버튼 클릭 후 re-Run")
            msg.exec_()
            
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
        categs = self.categ_toggled()
        if len(categs) != 1:
            msg = QMessageBox()
            msg.setText("** 한개의 카테고리를 선택하세요 **")
            msg.exec_()
        
        else:            
            # save category 
            with open(self.category_list, 'wb') as f:
                pickle.dump(categs, f)
            
            df_mapped = self.thread_crw._get_tbl()
            df_mapped = df_mapped.loc[df_mapped.category==categs[0]].reset_index(drop=True)
            df_mapped.to_csv(self.path_input_df)
            
            products = len(df_mapped)
            time = round(products * 10 / 3600, 2)
            self.Products.display(products)
            self.Time.display(time)
            
            msg = QMessageBox()
            msg.setText("Selection done!")
            msg.exec_()
            
    def _run(self):
        
        if not self.thread_crw.power:
            if os.path.isfile(self.path_input_df):
                msg = QMessageBox()
                msg.setText("- 인터넷 연결 확인 \n- VPN 연결 확인 \n- mac 자동 잠금 해제 확인")
                msg.exec_()
                self.thread_crw._get_category()
                self.thread_crw.power = True
                self.thread_crw.start()
            else:
                msg = QMessageBox()
                msg.setText("** Select 완료 후 시도하세요 **")
                msg.exec_()    
        else:
            pass
            
    def save_file(self):
        ''' save csv file '''
        
        # 캐시에 해당 파일이 존재할 때 저장
        if os.path.isfile(self.path_scrape_df):
            df = pd.read_csv(self.path_scrape_df)
            file_save = QFileDialog.getSaveFileName(self, "Save File", "", "csv file (*.csv)")
            
            if file_save[0] != "":
                df.to_csv(file_save[0], index=False)
        else:
            msg = QMessageBox()
            msg.setText('일시정지 후 다시 시도해주세요')
            msg.exec_()
            
    def tbl_viewer(self):
        ''' table viewer '''
        
        # 캐시에 테이블이 존재할 때 open table viewer
        if os.path.isfile(self.path_scrape_df):
            if self.viewer is None:
                self.viewer = TableViewer()
            else:
                self.viewer.close()
                self.viewer = TableViewer()
                
            self.viewer.show()
            self.viewer._loadFile('scrape_df.csv')
        else:
            msg = QMessageBox()
            msg.setText('일시정지 후 다시 시도해주세요')
            msg.exec_()
            
    def _upload_df(self):
        ''' Upload table into db '''    
        
        if os.path.exists(self.path_input_df):
            df = pd.read_csv(self.path_input_df)
            if len(df) == 0:
                ck, table_name = self.thread_crw._upload_df(comp=True)
            else:
                ck, table_name = self.thread_crw._upload_df(comp=False)
        else:
            ck, table_name = self.thread_crw._upload_df(comp=True)
        
        # db connection check
        if ck == 1:
            msg = QMessageBox()
            msg.setText(f"<테이블 업로드 완료>\n- {table_name}")
            msg.exec_()
            
        elif ck == -1:
            msg = QMessageBox()
            msg.setText(f"\n    ** db 연결 끊김 **\n\n- Upload failed: {table_name}\n\n- VPN, wifi 재연결 필요\n\n- Upload 버튼 클릭 후 re-Run")
            msg.exec_()