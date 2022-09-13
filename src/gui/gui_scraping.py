import os
import sys
import pickle
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
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
form_path = os.path.join(form_dir, 'scrapingWindow.ui')
form = uic.loadUiType(form_path)[0]

from access_database.access_db import AccessDataBase
from multithreading.thread_crawling_naver import ThreadCrawlingNvInfo
from gui.table_view import TableViewer

class ScrapingWindow(QMainWindow, form):
    ''' Product Info Crawling Window '''
    
    def __init__(self):
        super().__init__()    
        self.setupUi(self)
        self.setWindowTitle('Update Naver Beauty')
        self.viewer = None
        self.crawled_data_df = None
        
        # path
        self.path_prg = os.path.join(tbl_cache + '/prg_dict.txt')
        self.crawled_data = os.path.join(tbl_cache, 'crawled_data.csv')
        self.scraping_data = os.path.join(tbl_cache, 'scraping_data.txt')
        
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = AccessDataBase(conn[0], conn[1], conn[2])
        
        # category
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

        # thread
        self.thread_scrape = ThreadCrawlingNvInfo()
        self.thread_scrape.progress.connect(self.update_progress)
        
        # btn
        self.Accept.clicked.connect(self._accept)
        self.Run.clicked.connect(self._scraping)
        self.Pause.clicked.connect(self.thread_scrape.stop)
        self.View.clicked.connect(self._viewer)
        self.Save.clicked.connect(self._save)
    
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
        if not self.thread_scrape.power:
            with open(self.path_prg, 'wb') as f:
                pickle.dump(prg_dict_, f)
                
            if itm == tot:
                message = f"{per}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s} **Complete**"
                os.remove(self.path_prg)
                
                msg = QMessageBox()
                table_name = 'naver_beauty_product_info_final_version'
                msg.setText(f"<테이블 업로드 완료>\n- {table_name}")
                msg.exec_()
            else:
                message = f"{per}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s} **PAUSE**"
            self.statusbar.showMessage(message)
        
        # # ip 차단 및 db 연결 끊김 대응
        # if self.thread_scrape.check == 1:
        #     msg = QMessageBox()
        #     msg.setText("\n    ** ip 차단됨 **\n\n - VPN 나라변경 필요\n - wifi 재연결 필요")
        #     msg.exec_()
            
        # elif self.thread_scrape.check == 2:
        #     msg = QMessageBox()
        #     msg.setText("\n    ** db 연결 끊김 **\n\n - VPN, wifi 재연결 필요\n\n - Upload 버튼 클릭 후 re-Run")
        #     msg.exec_()
        
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
    
    def msg_event(self):
        info = QMessageBox.warning(
            self, "Caution", "저장되지 않은 스크레이핑 데이터가 존재합니다",
            QMessageBox.SaveAll | QMessageBox.Ignore, 
            QMessageBox.SaveAll
        )
        if info == QMessageBox.SaveAll:
            self._save()
        elif info == QMessageBox.Ignore:
            pass
        else:
            pass
            
    def _accept(self):
        ''' accept category and get crawled data '''
        
        if self.thread_scrape.power:
            msg = QMessageBox()
            msg.setText("** 데이터 크롤링 진행중입니다 **")
            msg.exec_()
        else:
            if self.crawled_data_df is None:
                self.crawled_data_df = self.thread_scrape.get_data()
            else:
                pass
            # 카테고리 선택
            categs = self.categ_toggled()
            
            if len(categs) == 0:
                msg = QMessageBox()
                msg.setText('** 한개 이상의 카테고리를 선택해주세요 **')
                msg.exec_()
                
            else:
                if os.path.isfile(tbl_cache + '/prg_dict.txt'):
                    # 진행률 캐시 삭제
                    os.remove(tbl_cache + '/prg_dict.txt')
                
                if os.path.isfile(self.scraping_data):
                    # 스크레이핑 캐시 삭제
                    self.msg_event()
                    os.remove(self.scraping_data)    
                
                # save crawled data
                crawled_data_df_categ = self.crawled_data_df.loc[self.crawled_data_df.category.isin(categs)].reset_index(drop=True)
                crawled_data_df_categ.to_csv(self.crawled_data, index=False)
                
                # diplay products * crawling time
                products = len(crawled_data_df_categ)
                time = round(products * 5 / 3600, 2)
                self.Products.display(products)
                self.Time.display(time)
            
    def _scraping(self):
        ''' Start Scraping thread '''
        
        if self.thread_scrape.power:
            msg = QMessageBox()
            msg.setText("** 데이터 크롤링 진행중입니다 **")
            msg.exec_()
        else:    
            if os.path.isfile(self.crawled_data):
                msg = QMessageBox()
                msg.setText("- 인터넷 연결 확인 \n- VPN 연결 확인 \n- 자동 잠금 해제 확인")
                msg.exec_()
                self.thread_scrape.power = True
                self.thread_scrape.start()
            else:
                msg = QMessageBox()
                msg.setText('** Accept 완료 후 시도하세요 **')
                msg.exec_()
        
    def save_file(self, file_name):
        ''' save csv file '''
        
        file_path = os.path.join(tbl_cache, file_name)
        # 캐시에 해당 파일이 존재할 때 저장
        if os.path.isfile(file_path):
            df = pd.read_csv(file_path)
            file_save = QFileDialog.getSaveFileName(self, "Save File", "", "csv file (*.csv)")
            
            if file_save[0] != "":
                df.to_csv(file_save[0], index=False)
        else:
            msg = QMessageBox()
            msg.setText('** 일시정지 후 다시 시도해주세요 **')
            msg.exec_()
            
    def tbl_viewer(self, file_name):
        ''' table viewer '''
        
        # 캐시에 테이블이 존재할 때 open table viewer
        file_path = os.path.join(tbl_cache, file_name)
        if os.path.isfile(file_path):
            if self.viewer is None:
                self.viewer = TableViewer()
            else:
                self.viewer.close()
                self.viewer = TableViewer()
                
            self.viewer.show()
            self.viewer._loadFile(file_name)
        else:
            msg = QMessageBox()
            msg.setText('** 일시정지 후 다시 시도해주세요 **')
            msg.exec_()
        
    def _save(self):
        file_name = "scraping_data.csv"
        self.save_file(file_name)
    
    def _viewer(self):
        file_name = "scraping_data.csv"
        self.tbl_viewer(file_name)